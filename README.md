# sqlite_zstd_rovfs

This SQLite extension enables it to read a finalized database file that's been compressed in the [Zstandard seekable format](https://github.com/facebook/zstd/tree/v1.4.4/contrib/seekable_format). It decompresses database pages just-in-time in order to serve SQLite's read requests.

## Quick start

Prerequisites: CMake >= 3.11 and modern packages for SQLite3 and Zstandard development (e.g. `apt install sqlite3 libsqlite3-dev zstd libzstd-dev`)

Fetch source code and compile:

```
git clone https://github.com/mlin/sqlite_zstd_rovfs.git
cd sqlite_zstd_rovfs

cmake -DCMAKE_BUILD_TYPE=Release -B build
cmake --build build
```

Download a ~1MB example database, compress it (with 4 KiB frame size and 4 threads), and confirm it decompresses identically:

```
wget https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite
build/zstd_seekable_compress Chinook_Sqlite.sqlite 4096 4
ls -sh Chinook_Sqlite.sqlite*
diff -s Chinook_Sqlite.sqlite <(zstd -dc Chinook_Sqlite.sqlite.zst)
```

Query the compressed database using `sqlite3` CLI:

```
sqlite3 :memory: -bail \
  -cmd '.load build/zstd_rovfs.so' \
  -cmd '.open file:Chinook_Sqlite.sqlite.zst?mode=ro&vfs=zstd_ro' \
  "select e.*, count(i.invoiceid) as 'Total Number of Sales'
    from employee as e
        join customer as c on e.employeeid = c.supportrepid
        join invoice as i on i.customerid = c.customerid
    group by e.employeeid"
```

Or in Python:

```
python3 - << 'EOF'
import sqlite3
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension("build/zstd_rovfs.so")
conn = sqlite3.connect("file:Chinook_Sqlite.sqlite.zst?mode=ro&vfs=zstd_ro")
for row in conn.execute("""
    select e.*, count(i.invoiceid) as 'Total Number of Sales'
    from employee as e
        join customer as c on e.employeeid = c.supportrepid
        join invoice as i on i.customerid = c.customerid
    group by e.employeeid
    """):
    print(row)
EOF
```
