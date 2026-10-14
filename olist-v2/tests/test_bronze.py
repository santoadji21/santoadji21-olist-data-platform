"""Test bronze tanpa Docker: S3 palsu di memori + CSV kecil buatan + kontrak ODCS asli."""
import hashlib
import io
import json
from datetime import datetime, timezone

import pytest

from olist_v2.bronze import file_facts, manifest_key, report_md, run_ingest
from olist_v2.contract import Table, load_tables


class Missing(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3:
    """Cukup untuk bronze: meniru ETag S3 (md5 untuk upload tunggal)."""

    def __init__(self, multipart_etag: bool = False):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.uploads = 0
        self.multipart_etag = multipart_etag

    def head_bucket(self, Bucket): pass
    def create_bucket(self, Bucket): pass

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Missing()
        md5 = hashlib.md5(self.objects[(Bucket, Key)]).hexdigest()
        return {"ETag": f'"{md5}-3"' if self.multipart_etag else f'"{md5}"'}

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
    t = Table("tiny", "tiny.csv", 2)
    (tmp_path / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,b\n', encoding="utf-8")
    return FakeS3(), t, tmp_path


def run(s3, t, d):
    return run_ingest(s3, d, [t])["files"][0]


def test_rows_counted_by_csv_parser_not_newlines(setup):
    _, _, d = setup
    assert file_facts(d / "tiny.csv").rows == 2   # wc -l akan bilang 4


def test_facts_have_both_checksums_matching_independent_hashes(setup):
    _, _, d = setup
    data = (d / "tiny.csv").read_bytes()
    f = file_facts(d / "tiny.csv")
    assert f.sha256 == hashlib.sha256(data).hexdigest() and f.md5 == hashlib.md5(data).hexdigest() and f.size_bytes == len(data)


def test_first_run_uploads_then_second_is_unchanged(setup):
    s3, t, d = setup
    assert run(s3, t, d)["status"] == "uploaded"
    assert run(s3, t, d)["status"] == "unchanged" and s3.uploads == 1


def test_manifest_records_sha256_md5_size_rows_time_and_versions(setup):
    s3, t, d = setup
    run(s3, t, d)
    v = json.loads(s3.objects[("bronze", manifest_key("tiny"))])["versions"][0]
    for k in ("sha256", "md5", "size_bytes", "rows", "ingested_at", "object_key", "version"):
        assert k in v
    assert v["rows"] == 2 and datetime.fromisoformat(v["ingested_at"]).tzinfo is not None


def test_etag_is_compared_with_md5_for_single_part_and_marked_none_for_multipart(setup):
    s3, t, d = setup
    run(s3, t, d)
    assert json.loads(s3.objects[("bronze", manifest_key("tiny"))])["versions"][0]["etag_matches_md5"] is True
    s3m = FakeS3(multipart_etag=True)
    run(s3m, t, d)
    assert json.loads(s3m.objects[("bronze", manifest_key("tiny"))])["versions"][0]["etag_matches_md5"] is None


def test_one_byte_change_is_new_version_and_old_object_kept(setup):
    s3, t, d = setup
    first = run(s3, t, d)
    (d / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,c\n', encoding="utf-8")
    second = run(s3, t, d)
    assert second["status"] == "new_version" and second["version"] == 2
    assert ("bronze", first["object_key"]) in s3.objects
    m = json.loads(s3.objects[("bronze", manifest_key("tiny"))])
    assert [v["version"] for v in m["versions"]] == [1, 2] and m["latest"]["object_key"] == second["object_key"]


def test_same_size_different_content_is_detected(setup):
    s3, t, d = setup
    run(s3, t, d)
    (d / "tiny.csv").write_text('id,komentar\n1,"baris\nbaru"\n2,z\n', encoding="utf-8")   # ukuran sama
    assert run(s3, t, d)["status"] == "new_version"


def test_same_day_different_content_does_not_overwrite(setup):
    s3, t, d = setup
    a = run(s3, t, d)
    (d / "tiny.csv").write_text('id,komentar\n1,x\n2,y\n', encoding="utf-8")
    b = run(s3, t, d)
    assert a["object_key"] != b["object_key"] and ("bronze", a["object_key"]) in s3.objects


def test_missing_source_stops_before_touching_bucket(setup):
    s3, t, d = setup
    (d / "tiny.csv").unlink()
    with pytest.raises(FileNotFoundError, match="tiny.csv"):
        run(s3, t, d)
    assert s3.objects == {}


def test_crash_before_manifest_is_recovered_without_reupload(setup):
    s3, t, d = setup
    run(s3, t, d)
    del s3.objects[("bronze", manifest_key("tiny"))]
    assert run(s3, t, d)["status"] == "uploaded" and s3.uploads == 1


def test_row_count_mismatch_is_reported_not_hidden(setup):
    s3, _, d = setup
    r = run(s3, Table("tiny", "tiny.csv", 99), d)
    assert r["rows_match"] is False and "⚠" in report_md({"bucket": "bronze", "ran_at": "x", "seconds": 0, "files": [r]})


def test_utf8_bom_file_is_counted_correctly(tmp_path):
    (tmp_path / "bom.csv").write_bytes(b"\xef\xbb\xbfa,b\n1,2\n")
    assert file_facts(tmp_path / "bom.csv").rows == 1


def test_tables_come_from_the_odcs_contract():
    tables = {t.name: t for t in load_tables()}
    assert len(tables) == 9
    assert tables["orders"].rows == 99_441 and tables["geolocation"].rows == 1_000_163
    assert tables["product_category_translation"].encoding == "utf-8-sig"          # BOM
    assert all(t.file.endswith(".csv") for t in tables.values())
