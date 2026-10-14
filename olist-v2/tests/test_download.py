"""Test unduhan Kaggle tanpa Docker dan tanpa jaringan: `fetch` palsu menulis ZIP buatan."""
import zipfile
from pathlib import Path

import pytest

from olist_v2.contract import Table
from olist_v2.download import DatasetError, check_credentials, download_dataset, extract_known

TABLES = [Table("a", "a.csv", 2), Table("b", "b.csv", 1)]
GOOD = {"a.csv": "id,x\n1,p\n2,q\n", "b.csv": "id\n1\n"}


def make_fetch(files, extra=(), calls=None):
    def fetch(dest: Path):
        if calls is not None:
            calls.append(1)
        with zipfile.ZipFile(dest / "brazilian-ecommerce.zip", "w") as z:
            for name, body in files.items():
                z.writestr(name, body)
            for name, body in extra:
                z.writestr(name, body)
    return fetch


def test_downloads_extracts_verifies_and_moves_files(tmp_path):
    r = download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(GOOD))
    assert r["status"] == "downloaded" and {f["file"] for f in r["files"]} == {"a.csv", "b.csv"}
    assert (tmp_path / "raw" / "a.csv").read_text() == GOOD["a.csv"]
    assert [p.name for p in (tmp_path / "raw").iterdir() if p.name.startswith(".dl-")] == []   # tidak ada sisa sementara


def test_second_run_is_present_and_does_not_download(tmp_path):
    calls = []
    download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(GOOD, calls=calls))
    r = download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(GOOD, calls=calls))
    assert r["status"] == "present" and len(calls) == 1


def test_force_downloads_again(tmp_path):
    calls = []
    download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(GOOD, calls=calls))
    download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(GOOD, calls=calls), force=True)
    assert len(calls) == 2


def test_row_count_mismatch_stops_and_leaves_raw_untouched(tmp_path):
    bad = {"a.csv": "id,x\n1,p\n", "b.csv": "id\n1\n"}                 # a.csv: 1 baris, kontrak 2
    with pytest.raises(DatasetError, match=r"a\.csv.*kontrak 2"):
        download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch(bad))
    assert [p.name for p in (tmp_path / "raw").iterdir() if p.suffix == ".csv"] == []   # tidak setengah terisi


def test_missing_file_in_zip_names_the_file(tmp_path):
    with pytest.raises(DatasetError, match="b.csv"):
        download_dataset(tmp_path / "raw", TABLES, fetch=make_fetch({"a.csv": GOOD["a.csv"]}))


def test_zip_slip_and_unknown_files_are_never_extracted(tmp_path):
    dest = tmp_path / "out"
    dest.mkdir()
    z = tmp_path / "x.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("../evil.csv", "x")
        zf.writestr("sub/a.csv", "x")
        zf.writestr("unknown.csv", "x")
        zf.writestr("a.csv", GOOD["a.csv"])
    assert extract_known(z, dest, {"a.csv", "b.csv"}) == ["a.csv"]
    assert sorted(p.name for p in dest.iterdir()) == ["a.csv"] and not (tmp_path / "evil.csv").exists()


def test_no_zip_from_kaggle_is_an_error(tmp_path):
    with pytest.raises(DatasetError, match="ZIP"):
        download_dataset(tmp_path / "raw", TABLES, fetch=lambda dest: None)


def test_missing_credentials_message_tells_where_to_put_the_key(tmp_path):
    with pytest.raises(DatasetError, match=r"olist-v2/\.kaggle"):
        check_credentials({"KAGGLE_CONFIG_DIR": str(tmp_path)})
    (tmp_path / "kaggle.json").write_text("{}")
    assert check_credentials({"KAGGLE_CONFIG_DIR": str(tmp_path)}) == tmp_path / "kaggle.json"
    assert check_credentials({"KAGGLE_CONFIG_DIR": str(tmp_path / "none"), "KAGGLE_USERNAME": "u", "KAGGLE_KEY": "k"})
