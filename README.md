# dbite

A minimal key-value database library implementing a copy-on-write B+ tree, inspired by LMDB. Unlike LMDB, this library uses `pread`/`pwrite` for disk access instead of memory-mapped files.

### Getting Started

Build the Library

```bash
# clone the repo and cd to it
# and then compile the lib
make
```

Build and Run Tests

```bash
cd tests
make
./test
```

You can play around with the tests located in `tests/tests.cpp`.

### Notes

- This is a Minimal. Educational DB. maybe i'll make a good thing out of it maybe not i don't know
- Works on Linux (arch btw). Other OSes might explode, or might not. Who knows?
