"""Test bronze tanpa Docker: S3 palsu di memori + CSV kecil buatan."""
import io
import json
from datetime import datetime, timezone

import pytest

from olist.bronze import file_facts, manifest_key, run_ingest
from olist.schema import Table


class Missing(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.uploads = 0

    def head_bucket(self, Bucket): pass
    def create_bucket(self, Bucket): pass

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Missing()

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Missing()
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[(Bucket, Key)] = Body

    def upload_file(self, path, Bucket, Key, **kw):
        self.uploads += 1
        self.objects[(Bucket, Key)] = open(path, "rb").read()


@pytest.fixture
def setup(tmp_path):
    t = Table("tiny", "tiny.csv", 2, (), "tabel uji", ())
    (tmp_path / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,b\n', encoding="utf-8")
    return FakeS3(), t, tmp_path


def run(s3, t, d):
    return run_ingest(s3, "bronze", d, tables=(t,))["files"][0]


def test_rows_counted_by_csv_parser_not_newlines(setup):
    _, _, d = setup
    assert file_facts(d / "tiny.csv").rows == 2  # wc -l akan bilang 4


def test_first_run_uploads_then_second_is_unchanged(setup):
    s3, t, d = setup
    assert run(s3, t, d)["status"] == "uploaded"
    assert run(s3, t, d)["status"] == "unchanged"
    assert s3.uploads == 1


def test_one_byte_change_is_new_version_and_old_object_kept(setup):
    s3, t, d = setup
    first = run(s3, t, d)
    (d / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,c\n', encoding="utf-8")
    second = run(s3, t, d)
    assert second["status"] == "new_version" and second["version"] == 2
    assert ("bronze", first["object_key"]) in s3.objects  # versi lama tetap ada
    m = json.loads(s3.objects[("bronze", manifest_key("tiny"))])
    assert [v["version"] for v in m["versions"]] == [1, 2]
    assert m["latest"]["object_key"] == second["object_key"]


def test_same_size_different_content_is_detected(setup):
    s3, t, d = setup
    run(s3, t, d)
    (d / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,z\n', encoding="utf-8")  # ukuran sama
    assert run(s3, t, d)["status"] == "new_version"


def test_missing_source_stops_before_touching_bucket(setup):
    s3, t, d = setup
    (d / "tiny.csv").unlink()
    with pytest.raises(FileNotFoundError, match="tiny.csv"):
        run(s3, t, d)
    assert s3.objects == {}


def test_crash_before_manifest_is_recovered_by_rerun(setup):
    s3, t, d = setup
    run(s3, t, d)
    del s3.objects[("bronze", manifest_key("tiny"))]  # simulasi mati sebelum manifest tertulis
    assert run(s3, t, d)["status"] == "uploaded"
    assert s3.uploads == 1  # objek bersandi-isi sudah ada, tidak diunggah ulang
