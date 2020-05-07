/*
** SQLite read-only, zstd-compressed VFS extension
**
** This extension enables SQLite to read a database file that's been compressed in the "seekable"
** Zstandard format:
**     https://github.com/facebook/zstd/tree/v1.4.4/contrib/seekable_format
** The VFS decompresses frames of the file just-in-time to serve SQLite's read requests.
**
** This source file originally adapted from: https://www.sqlite.org/src/file/ext/misc/memvfs.c
**
** References:
** - https://www.sqlite.org/capi3ref.html
** - https://www.sqlite.org/c3ref/constlist.html
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <zstd_seekable.h>
#ifndef NDEBUG
#include <pthread.h>
#endif

/* Access to the inner/default vfs */
#define ORIGVFS(p) ((sqlite3_vfs *)((p)->pAppData))

/* An open file */
typedef struct {
    sqlite3_file base;           /* IO methods (must be first) */
    sqlite3_file *origfile;      /* compressed file as opened through ORIGVFS */
    sqlite3_int64 origfile_size; /* memoized size of the compressed file */
    sqlite3_int64 origfile_pos;  /* current uncompressed seek position in file */
    ZSTD_seekable *decompressor; /* Zstandard seekable decompression context */
#ifndef NDEBUG
    pthread_t seek_thread; /* ID of most recent thread to update origfile_pos */
#endif
} zstd_ro_file;

/*
** Methods for zstd_ro_file
*/
static int zstd_ro_Close(sqlite3_file *);
static int zstd_ro_Read(sqlite3_file *, void *, int iAmt, sqlite3_int64 iOfst);
static int zstd_ro_Write(sqlite3_file *, const void *, int iAmt, sqlite3_int64 iOfst);
static int zstd_ro_Truncate(sqlite3_file *, sqlite3_int64 size);
static int zstd_ro_Sync(sqlite3_file *, int flags);
static int zstd_ro_FileSize(sqlite3_file *, sqlite3_int64 *pSize);
static int zstd_ro_Lock(sqlite3_file *, int);
static int zstd_ro_Unlock(sqlite3_file *, int);
static int zstd_ro_CheckReservedLock(sqlite3_file *, int *pResOut);
static int zstd_ro_FileControl(sqlite3_file *, int op, void *pArg);
static int zstd_ro_SectorSize(sqlite3_file *);
static int zstd_ro_DeviceCharacteristics(sqlite3_file *);
static int zstd_ro_ShmMap(sqlite3_file *, int iPg, int pgsz, int, void volatile **);
static int zstd_ro_ShmLock(sqlite3_file *, int offset, int n, int flags);
static void zstd_ro_ShmBarrier(sqlite3_file *);
static int zstd_ro_ShmUnmap(sqlite3_file *, int deleteFlag);
static int zstd_ro_Fetch(sqlite3_file *, sqlite3_int64 iOfst, int iAmt, void **pp);
static int zstd_ro_Unfetch(sqlite3_file *, sqlite3_int64 iOfst, void *p);

/*
** Methods for vfs
*/
static int zstd_ro_Open(sqlite3_vfs *, const char *, sqlite3_file *, int, int *);
static int zstd_ro_Delete(sqlite3_vfs *, const char *zName, int syncDir);
static int zstd_ro_Access(sqlite3_vfs *, const char *zName, int flags, int *);
static int zstd_ro_FullPathname(sqlite3_vfs *, const char *zName, int, char *zOut);
static void *zstd_ro_DlOpen(sqlite3_vfs *, const char *zFilename);
static void zstd_ro_DlError(sqlite3_vfs *, int nByte, char *zErrMsg);
static void (*zstd_ro_DlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void);
static void zstd_ro_DlClose(sqlite3_vfs *, void *);
static int zstd_ro_Randomness(sqlite3_vfs *, int nByte, char *zOut);
static int zstd_ro_Sleep(sqlite3_vfs *, int microseconds);
static int zstd_ro_CurrentTime(sqlite3_vfs *, double *);
static int zstd_ro_GetLastError(sqlite3_vfs *, int, char *);
static int zstd_ro_CurrentTimeInt64(sqlite3_vfs *, sqlite3_int64 *);

static sqlite3_vfs zstd_rovfs = {
    2,                       /* iVersion */
    0,                       /* szOsFile (set when registered) */
    1024,                    /* mxPathname */
    0,                       /* pNext */
    "zstd_ro",               /* zName */
    0,                       /* pAppData (set when registered) */
    zstd_ro_Open,            /* xOpen */
    zstd_ro_Delete,          /* xDelete */
    zstd_ro_Access,          /* xAccess */
    zstd_ro_FullPathname,    /* xFullPathname */
    zstd_ro_DlOpen,          /* xDlOpen */
    zstd_ro_DlError,         /* xDlError */
    zstd_ro_DlSym,           /* xDlSym */
    zstd_ro_DlClose,         /* xDlClose */
    zstd_ro_Randomness,      /* xRandomness */
    zstd_ro_Sleep,           /* xSleep */
    zstd_ro_CurrentTime,     /* xCurrentTime */
    zstd_ro_GetLastError,    /* xGetLastError */
    zstd_ro_CurrentTimeInt64 /* xCurrentTimeInt64 */
};

static const sqlite3_io_methods zstd_ro_methods = {
    3,                             /* iVersion */
    zstd_ro_Close,                 /* xClose */
    zstd_ro_Read,                  /* xRead */
    zstd_ro_Write,                 /* xWrite */
    zstd_ro_Truncate,              /* xTruncate */
    zstd_ro_Sync,                  /* xSync */
    zstd_ro_FileSize,              /* xFileSize */
    zstd_ro_Lock,                  /* xLock */
    zstd_ro_Unlock,                /* xUnlock */
    zstd_ro_CheckReservedLock,     /* xCheckReservedLock */
    zstd_ro_FileControl,           /* xFileControl */
    zstd_ro_SectorSize,            /* xSectorSize */
    zstd_ro_DeviceCharacteristics, /* xDeviceCharacteristics */
    zstd_ro_ShmMap,                /* xShmMap */
    zstd_ro_ShmLock,               /* xShmLock */
    zstd_ro_ShmBarrier,            /* xShmBarrier */
    zstd_ro_ShmUnmap,              /* xShmUnmap */
    zstd_ro_Fetch,                 /* xFetch */
    zstd_ro_Unfetch                /* xUnfetch */
};

/*************************************************************************************************/

static int zstd_ro_Close(sqlite3_file *pFile) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    if (p->decompressor) {
        ZSTD_seekable_free(p->decompressor);
        p->decompressor = 0;
    }
    if (p->origfile) {
        if (p->origfile->pMethods) {
            p->origfile->pMethods->xClose(p->origfile);
        }
        sqlite3_free(p->origfile);
        p->origfile = 0;
    }
    return SQLITE_OK;
}

static int zstd_ro_Read(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst) {
    if (iAmt < 0 || iOfst < 0) {
        return SQLITE_IOERR_READ;
    }
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    size_t zrc =
        ZSTD_seekable_decompress(p->decompressor, zBuf, (size_t)iAmt, (unsigned long long)iOfst);
    char *b = (char *)zBuf;
    /*printf("zstd_ro_Read(n=%d,pos=%lld) -> %llu\n", iAmt, iOfst, zrc);*/
    if (zrc == (size_t)iAmt) {
        return SQLITE_OK;
    }
    if (zrc < (size_t)iAmt) {
        memset(&((char *)zBuf)[zrc], 0, (size_t)iAmt - zrc);
        return SQLITE_IOERR_SHORT_READ;
    }
    return SQLITE_IOERR_READ;
}

static int zstd_ro_Write(sqlite3_file *pFile, const void *z, int iAmt, sqlite_int64 iOfst) {
    return SQLITE_READONLY;
}

static int zstd_ro_Truncate(sqlite3_file *pFile, sqlite_int64 size) { return SQLITE_READONLY; }

static int zstd_ro_Sync(sqlite3_file *pFile, int flags) { return SQLITE_OK; }

static int zstd_ro_FileSize(sqlite3_file *pFile, sqlite_int64 *pSize) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    *pSize = 0;
    unsigned num_frames = ZSTD_seekable_getNumFrames(p->decompressor);
    if (num_frames) {
        *pSize = ZSTD_seekable_getFrameDecompressedOffset(p->decompressor, num_frames - 1) +
                 ZSTD_seekable_getFrameDecompressedSize(p->decompressor, num_frames - 1);
    }
    return SQLITE_OK;
}

static int zstd_ro_Lock(sqlite3_file *pFile, int eLock) { return SQLITE_OK; }

static int zstd_ro_Unlock(sqlite3_file *pFile, int eLock) { return SQLITE_OK; }

static int zstd_ro_CheckReservedLock(sqlite3_file *pFile, int *pResOut) {
    *pResOut = 0;
    return SQLITE_OK;
}

static int zstd_ro_FileControl(sqlite3_file *pFile, int op, void *pArg) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    int rc = SQLITE_OK;
    switch (op) {
    case SQLITE_FCNTL_VFSNAME:
        *(char **)pArg = sqlite3_mprintf("zstd_ro");
        break;
    case SQLITE_FCNTL_FILE_POINTER:
        *(sqlite3_file **)pArg = pFile;
        break;
    case SQLITE_FCNTL_VFS_POINTER:
        *(sqlite3_vfs **)pArg = &zstd_rovfs;
        break;
    case SQLITE_FCNTL_TEMPFILENAME:
    case SQLITE_FCNTL_HAS_MOVED:
        rc = p->origfile->pMethods->xFileControl(p->origfile, op, pArg);
        break;
    default:
        rc = SQLITE_NOTFOUND;
    }
    return rc;
}

static int zstd_ro_SectorSize(sqlite3_file *pFile) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    int ans = 4096;
    /* take size of first frame if possible */
    size_t zrc = ZSTD_seekable_getFrameDecompressedSize(p->decompressor, 0);
    if (!ZSTD_isError(zrc) && zrc <= ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE) {
        ans = (int)zrc;
    }
    return ans;
}

static int zstd_ro_DeviceCharacteristics(sqlite3_file *pFile) { return 0; }

static int zstd_ro_ShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp) {
    return SQLITE_IOERR_SHMMAP;
}

static int zstd_ro_ShmLock(sqlite3_file *pFile, int offset, int n, int flags) {
    return SQLITE_IOERR_SHMLOCK;
}

static void zstd_ro_ShmBarrier(sqlite3_file *pFile) { return; }

static int zstd_ro_ShmUnmap(sqlite3_file *pFile, int deleteFlag) { return SQLITE_OK; }

static int zstd_ro_Fetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp) {
    *pp = NULL;
    return SQLITE_IOERR_MMAP;
}

static int zstd_ro_Unfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage) {
    return SQLITE_IOERR_MMAP;
}

/*************************************************************************************************/

/* Hook up ZSTD_seekable to read the compressed file via ORIGVFS */
static int ZSTD_seekable_read_sqlite3_file(void *pFile, void *buffer, size_t n) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    /* debug assert: most recent seek(), if any, should be from the same thread */
    assert(!p->seek_thread || p->seek_thread == pthread_self());
    int rc = p->origfile->pMethods->xRead(p->origfile, buffer, (int)n, p->origfile_pos);
    /*printf("ZSTD_seekable_read_sqlite3_file(pos=%lld, n=%lu) -> %d\n", p->origfile_pos, n, rc);*/
    if (rc == SQLITE_IOERR_SHORT_READ) {
        assert(p->origfile_pos + n > p->origfile_size);
        int ans = p->origfile_pos < p->origfile_size ? p->origfile_size - p->origfile_pos : 0;
        p->origfile_pos += ans;
        return ans;
    } else if (rc == SQLITE_OK) {
        p->origfile_pos += n;
        return (int)n;
    }
    return -1;
}

static int ZSTD_seekable_seek_sqlite3_file(void *pFile, long long offset, int origin) {
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    assert(p->seek_thread = pthread_self()); /* intentional single = */
    if (origin == SEEK_SET && offset >= 0) {
        p->origfile_pos = offset;
    } else if (origin == SEEK_CUR && p->origfile_pos + offset >= 0) {
        p->origfile_pos += offset;
    } else if (origin == SEEK_END && p->origfile_size + offset >= 0) {
        p->origfile_pos = p->origfile_size + offset;
    } else {
        assert(0);
        return -1;
    }
    /*printf("ZSTD_seekable_seek_sqlite3_file(%lld, %d, %lld) -> %lld\n", offset, origin,
     * p->origfile_size, p->origfile_pos);*/
    /* TODO for debugging -- put a sleep here to ensure useful test of the race condition */
    return 0;
}

/*************************************************************************************************/

static int zstd_ro_Open(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags,
                        int *pOutFlags) {
    assert(pVfs == &zstd_rovfs);

    /* validate flags */
    if (!(flags & SQLITE_OPEN_READONLY) || (flags & (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE))) {
        return SQLITE_READONLY;
    }
    if ((flags & SQLITE_OPEN_MAIN_DB) == 0) {
        return SQLITE_CANTOPEN;
    }

    /* setup */
    zstd_ro_file *p = (zstd_ro_file *)pFile;
    memset(p, 0, sizeof(*p));
    p->base.pMethods = &zstd_ro_methods; /* whatever happens, zstd_ro_Close() will now be called */

    /* open compressed file via ORIGVFS */
    int rc = SQLITE_CANTOPEN;
    p->origfile = sqlite3_malloc(ORIGVFS(pVfs)->szOsFile);
    if (!p->origfile)
        return SQLITE_NOMEM;
    rc = ORIGVFS(pVfs)->xOpen(ORIGVFS(pVfs), zName, p->origfile, flags, pOutFlags);
    if (rc != SQLITE_OK)
        return rc;

    rc = p->origfile->pMethods->xFileSize(p->origfile, &p->origfile_size);
    if (rc != SQLITE_OK)
        return rc;
    assert(p->origfile_size >= 0);

    /* initialize zstd decompressor */
    p->decompressor = ZSTD_seekable_create();
    if (!p->decompressor)
        return SQLITE_NOMEM;
    ZSTD_seekable_customFile zcf = {p, ZSTD_seekable_read_sqlite3_file,
                                    ZSTD_seekable_seek_sqlite3_file};
    size_t zrc = ZSTD_seekable_initAdvanced(p->decompressor, zcf);
    return ZSTD_isError(zrc) ? SQLITE_CORRUPT : SQLITE_OK;
}

static int zstd_ro_Delete(sqlite3_vfs *pVfs, const char *zPath, int dirSync) {
    return SQLITE_READONLY;
}

/* Remaining methods shim ORIGVFS */
static int zstd_ro_Access(sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut) {
    assert(pVfs == &zstd_rovfs);
    if (flags & SQLITE_ACCESS_READWRITE) {
        *pResOut = 0;
        return SQLITE_OK;
    }
    return ORIGVFS(pVfs)->xAccess(ORIGVFS(pVfs), zPath, flags, pResOut);
}

static int zstd_ro_FullPathname(sqlite3_vfs *pVfs, const char *zPath, int nOut, char *zOut) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xFullPathname(ORIGVFS(pVfs), zPath, nOut, zOut);
}

static void *zstd_ro_DlOpen(sqlite3_vfs *pVfs, const char *zPath) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zPath);
}

static void zstd_ro_DlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) {
    assert(pVfs == &zstd_rovfs);
    ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}

static void (*zstd_ro_DlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}

static void zstd_ro_DlClose(sqlite3_vfs *pVfs, void *pHandle) {
    assert(pVfs == &zstd_rovfs);
    ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}

static int zstd_ro_Randomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zBufOut);
}

static int zstd_ro_Sleep(sqlite3_vfs *pVfs, int nMicro) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), nMicro);
}

static int zstd_ro_CurrentTime(sqlite3_vfs *pVfs, double *pTimeOut) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTimeOut);
}

static int zstd_ro_GetLastError(sqlite3_vfs *pVfs, int a, char *b) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}
static int zstd_ro_CurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *p) {
    assert(pVfs == &zstd_rovfs);
    return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), p);
}

/*************************************************************************************************/

/*
** This routine is called when the extension is loaded.
** Register the new VFS.
*/
int sqlite3_zstdrovfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);
    zstd_rovfs.pAppData = sqlite3_vfs_find(0);
    zstd_rovfs.szOsFile = sizeof(zstd_ro_file);
    rc = sqlite3_vfs_register(&zstd_rovfs, 0);
    if (rc == SQLITE_OK)
        rc = SQLITE_OK_LOAD_PERMANENTLY;
    return rc;
}
