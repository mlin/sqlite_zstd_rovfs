# sqlite_zstd_rovfs

This extension enables SQLite to read a database file that's been compressed in the [Zstandard seekable format](https://github.com/facebook/zstd/tree/v1.4.4/contrib/seekable_format). To serve SQLite's read requests, it decompresses frames of the file just-in-time.

```
cmake -DCMAKE_BUILD_TYPE=Debug -B build
cmake --build build
env -C build ctest -V

cmake --build build --target pretty
```
