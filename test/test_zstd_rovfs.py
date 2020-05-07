import sys
import os
import subprocess
import multiprocessing
import tempfile
import sqlite3
import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))


@pytest.fixture(scope="session")
def chinook_file(tmpdir_factory):
    dn = tmpdir_factory.mktemp("chinook")
    subprocess.run(
        [
            "wget",
            "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        ],
        check=True,
        cwd=dn,
    )
    return str(dn.join("Chinook_Sqlite.sqlite"))

@pytest.fixture(scope="session")
def chinook_file_zst(chinook_file):
    subprocess.run(
        [
            os.path.join(BUILD, "zstd_seekable_compress"),
            chinook_file,
            "4096",
            str(multiprocessing.cpu_count()),
        ],
        check=True,
    )
    return chinook_file+".zst"

def test_chinook(tmpdir, chinook_file, chinook_file_zst):
    # digest the SQL dump
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.open --readonly file:{chinook_file}' -cmd .dump -cmd .exit | sha256sum",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    expected = rslt.stdout.strip()

    # dump from the compressed version
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD,'zstd_rovfs.so')}' -cmd '.open --readonly file:{chinook_file_zst}?vfs=zstd_ro' -cmd .dump -cmd .exit | sha256sum",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    assert rslt.stdout.strip() == expected

    # attempt to read non-zstd file shouldn't crash:
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD,'zstd_rovfs.so')}' -cmd '.open --readonly file:{chinook_file}?vfs=zstd_ro' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert "database disk image is malformed" in rslt.stderr

    # verify .zst decompresses identically to original:
    subprocess.run(
        f"/bin/bash -c 'cmp {chinook_file} <(zstd -dc {chinook_file}.zst)'",
        check=True,
        shell=True,
        cwd=tmpdir,
    )

def test_python_load(chinook_file_zst):
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.execute(f"select load_extension('{os.path.join(BUILD,'zstd_rovfs.so')}')")
    con = sqlite3.connect(f"file:{chinook_file_zst}?vfs=zstd_ro&mode=ro")
    assert len(list(con.execute("select * from Employee"))) == 8
    with pytest.raises(sqlite3.OperationalError):
        con = sqlite3.connect(f"file:bogus123?vfs=zstd_ro&mode=ro")


# black -l 100 test/test_zstd_rovfs.py 
