package rgw

/*
#cgo LDFLAGS: -lrgw
#include <stdlib.h>
#include <sys/stat.h>
#include <rados/librgw.h>
#include <rados/rgw_file.h>

// readdir_callback.go
extern bool common_readdir_cb(const char *name, void *arg, uint64_t offset,
                       struct stat *st, uint32_t mask,
                       uint32_t flags);
*/
import "C"

import (
	"syscall"
	"unsafe"

	gopointer "github.com/mattn/go-pointer"
)

// typedef void* librgw_t;
type RGW struct {
	libRGW *C.librgw_t
}

// int librgw_create(librgw_t *rgw, int argc, char **argv)
func createRGW(argc C.int, argv **C.char) (*RGW, error) {
	rgw := &RGW{}
	var libRGW C.librgw_t
	if ret := C.librgw_create(&libRGW, argc, argv); ret == 0 {
		rgw.libRGW = &libRGW
		return rgw, nil
	} else {
		return nil, getError(ret)
	}
}

func CreateRGW(argv []string) (*RGW, error) {
	cargv := make([]*C.char, len(argv))
	for i := range argv {
		cargv[i] = C.CString(argv[i])
		defer C.free(unsafe.Pointer(cargv[i]))
	}

	return createRGW(C.int(len(cargv)), &cargv[0])
}

// void librgw_shutdown(librgw_t rgw)
func ShutdownRGW(rgw *RGW) {
	C.librgw_shutdown(*rgw.libRGW)
}

// FS exports ceph's rgw_fs from include/rados/rgw_file.h
type FS struct {
	rgwFS *C.struct_rgw_fs
}

// int rgw_mount(librgw_t rgw, const char *uid, const char *key,
//               const char *secret, rgw_fs **fs, uint32_t flags)
func (fs *FS) Mount(rgw *RGW, uid, key, secret string, flags uint32) error {
	cuid := C.CString(uid)
	ckey := C.CString(key)
	csecret := C.CString(secret)

	defer C.free(unsafe.Pointer(cuid))
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(csecret))
	ret := C.rgw_mount(*rgw.libRGW, cuid, ckey, csecret,
		&fs.rgwFS, C.uint(flags))
	if ret != 0 {
		return getError(ret)
	}

	return nil
}

// int rgw_umount(rgw_fs *fs, uint32_t flags)
func (fs *FS) Umount(flags uint32) error {
	if ret := C.rgw_umount(fs.rgwFS, C.uint(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}

}

// StatVFS instances are returned from the StatFS call. It reports
// file-system wide statistics.
type StatVFS struct {
	// Bsize reports the file system's block size.
	Bsize int64
	// Fragment reports the file system's fragment size.
	Frsize int64
	// Blocks reports the number of blocks in the file system.
	Blocks uint64
	// Bfree reports the number of free blocks.
	Bfree uint64
	// Bavail reports the number of free blocks for unprivileged users.
	Bavail uint64
	// Files reports the number of inodes in the file system.
	Files uint64
	// Ffree reports the number of free indoes.
	Ffree uint64
	// Favail reports the number of free indoes for unprivileged users.
	Favail uint64
	// Fsid reports the file system ID number.
	Fsid [2]int64
	// Flag reports the file system mount flags.
	Flag int64
	// Namemax reports the maximum file name length.
	Namemax int64
}

// struct rgw_file_handle
type FileHandle struct {
	handle *C.struct_rgw_file_handle
}

func (fs *FS) GetRootFileHandle() *FileHandle {
	return &FileHandle{
		handle: fs.rgwFS.root_fh,
	}
}

//    int rgw_statfs(rgw_fs *fs, rgw_file_handle *parent_fh,
//                   rgw_statvfs *vfs_st, uint32_t flags)
func (fs *FS) StatFS(pFH *FileHandle, flags uint32) (*StatVFS, error) {
	var statVFS C.struct_rgw_statvfs
	if ret := C.rgw_statfs(fs.rgwFS, pFH.handle, &statVFS, C.uint(flags)); ret != 0 {
		return nil, getError(ret)
	} else {
		stat := &StatVFS{
			Bsize:   int64(statVFS.f_bsize),
			Frsize:  int64(statVFS.f_frsize),
			Blocks:  uint64(statVFS.f_blocks),
			Bfree:   uint64(statVFS.f_bfree),
			Bavail:  uint64(statVFS.f_bavail),
			Files:   uint64(statVFS.f_files),
			Ffree:   uint64(statVFS.f_ffree),
			Favail:  uint64(statVFS.f_favail),
			Fsid:    [2]int64{int64(statVFS.f_fsid[0]), int64(statVFS.f_fsid[1])},
			Flag:    int64(statVFS.f_flag),
			Namemax: int64(statVFS.f_namemax),
		}
		return stat, nil
	}
}

// syscall.Stat_t
// type Stat_t struct {
//     Dev       uint64
//     Ino       uint64
//     Nlink     uint64
//     Mode      uint32
//     Uid       uint32
//     Gid       uint32
//     X__pad0   int32
//     Rdev      uint64
//     Size      int64
//     Blksize   int64
//     Blocks    int64
//     Atim      Timespec
//     Mtim      Timespec
//     Ctim      Timespec
//     X__unused [3]int64
// }
// typedef bool (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset,
//                                struct stat *st, uint32_t mask,
//                                uint32_t flags);

type ReadDirCallback interface {
	Callback(name string, st *syscall.Stat_t, mask, flags uint32) bool
}

//export goCommonReadDirCallback
func goCommonReadDirCallback(name *C.char, arg unsafe.Pointer, offset C.uint64_t,
	stat *C.struct_stat, mask, flags C.uint32_t) bool {

	cb := gopointer.Restore(arg).(ReadDirCallback)

	var st syscall.Stat_t
	if stat != nil {
		st = syscall.Stat_t{
			Dev:     uint64(stat.st_dev),
			Ino:     uint64(stat.st_ino),
			Nlink:   uint64(stat.st_nlink),
			Mode:    uint32(stat.st_mode),
			Uid:     uint32(stat.st_uid),
			Gid:     uint32(stat.st_gid),
			Rdev:    uint64(stat.st_rdev),
			Size:    int64(stat.st_size),
			Blksize: int64(stat.st_blksize),
			Blocks:  int64(stat.st_blocks),
			// FIXME
			//	st.Atim = st.st_atime
			//	st.Mtim = st.st_mtime
			//	st.Ctim = st.st_ctime
		}
	}
	return cb.Callback(C.GoString(name), &st, uint32(mask), uint32(flags))
}

// int rgw_readdir(struct rgw_fs *rgw_fs,
//                 struct rgw_file_handle *parent_fh, uint64_t *offset,
//                 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
//                 uint32_t flags)
func (fs *FS) ReadDir(parentHdl *FileHandle, cb ReadDirCallback, offset uint64, flags uint32) (uint64, bool, error) {
	coffset := C.uint64_t(offset)
	var eof C.bool = false

	cbArg := gopointer.Save(cb)
	defer gopointer.Unref(cbArg)

	if ret := C.rgw_readdir(fs.rgwFS, parentHdl.handle, &coffset, C.rgw_readdir_cb(C.common_readdir_cb),
		unsafe.Pointer(cbArg), &eof, C.uint(flags)); ret != 0 {
		return 0, false, getError(ret)
	} else {
		next := uint64(coffset)
		return next, bool(eof), nil
	}
}

// void rgwfile_version(int *major, int *minor, int *extra)
func (fs *FS) Version() (int, int, int) {
	var major, minor, extra C.int
	C.rgwfile_version(&major, &minor, &extra)
	return int(major), int(minor), int(extra)
}

//    int rgw_lookup(rgw_fs *fs,
//                   rgw_file_handle *parent_fh, const char *path,
//                   rgw_file_handle **fh, stat* st, uint32_t st_mask,
//                   uint32_t flags)
func (fs *FS) Lookup(parentHdl *FileHandle, path string, stMask, flags uint32) (*FileHandle, *syscall.Stat_t, error) {
	var fh *FileHandle = &FileHandle{}
	var stat C.struct_stat

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	if ret := C.rgw_lookup(fs.rgwFS, parentHdl.handle, cPath, &fh.handle, &stat,
		C.uint32_t(stMask), C.uint32_t(flags)); ret == 0 {
		st := syscall.Stat_t{
			Dev:     uint64(stat.st_dev),
			Ino:     uint64(stat.st_ino),
			Nlink:   uint64(stat.st_nlink),
			Mode:    uint32(stat.st_mode),
			Uid:     uint32(stat.st_uid),
			Gid:     uint32(stat.st_gid),
			Rdev:    uint64(stat.st_rdev),
			Size:    int64(stat.st_size),
			Blksize: int64(stat.st_blksize),
			Blocks:  int64(stat.st_blocks),
			// FIXME
			//	st.Atim = st.st_atime
			//	st.Mtim = st.st_mtime
			//	st.Ctim = st.st_ctime
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

//    int rgw_create(rgw_fs *fs, rgw_file_handle *parent_fh,
//                   const char *name, stat *st, uint32_t mask,
//                   rgw_file_handle **fh, uint32_t posix_flags,
//                   uint32_t flags)
func (fs *FS) Create(parentHdl *FileHandle, name string, mask, posixFlags, flags uint32) (
	*FileHandle, *syscall.Stat_t, error) {

	var fh *FileHandle = &FileHandle{}
	var stat C.struct_stat

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	if ret := C.rgw_create(fs.rgwFS, parentHdl.handle, cName, &stat, C.uint32_t(mask), &fh.handle,
		C.uint32_t(posixFlags), C.uint32_t(flags)); ret == 0 {
		st := syscall.Stat_t{
			Dev:     uint64(stat.st_dev),
			Ino:     uint64(stat.st_ino),
			Nlink:   uint64(stat.st_nlink),
			Mode:    uint32(stat.st_mode),
			Uid:     uint32(stat.st_uid),
			Gid:     uint32(stat.st_gid),
			Rdev:    uint64(stat.st_rdev),
			Size:    int64(stat.st_size),
			Blksize: int64(stat.st_blksize),
			Blocks:  int64(stat.st_blocks),
			// FIXME
			//	st.Atim = st.st_atime
			//	st.Mtim = st.st_mtime
			//	st.Ctim = st.st_ctime
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

//    int rgw_mkdir(rgw_fs *fs,
//                  rgw_file_handle *parent_fh,
//                  const char *name, stat *st, uint32_t mask,
//                  rgw_file_handle **fh, uint32_t flags)
//
func (fs *FS) Mkdir(parentHdl *FileHandle, name string, mask, flags uint32) (
	*FileHandle, *syscall.Stat_t, error) {

	var fh *FileHandle = &FileHandle{}
	var stat C.struct_stat

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	if ret := C.rgw_mkdir(fs.rgwFS, parentHdl.handle, cName, &stat, C.uint32_t(mask), &fh.handle,
		C.uint32_t(flags)); ret == 0 {
		st := syscall.Stat_t{
			Dev:     uint64(stat.st_dev),
			Ino:     uint64(stat.st_ino),
			Nlink:   uint64(stat.st_nlink),
			Mode:    uint32(stat.st_mode),
			Uid:     uint32(stat.st_uid),
			Gid:     uint32(stat.st_gid),
			Rdev:    uint64(stat.st_rdev),
			Size:    int64(stat.st_size),
			Blksize: int64(stat.st_blksize),
			Blocks:  int64(stat.st_blocks),
			// FIXME
			//	st.Atim = st.st_atime
			//	st.Mtim = st.st_mtime
			//	st.Ctim = st.st_ctime
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

//    int rgw_write(rgw_fs *fs,
//                  rgw_file_handle *fh, uint64_t offset,
//                  size_t length, size_t *bytes_written, void *buffer,
//                  uint32_t flags)
//
func (fs *FS) Write(fh *FileHandle, buffer []byte, offset uint64, length uint64,
	flags uint32) (bytesWritten uint, err error) {
	var written C.size_t

	/// TODO: handle zero length buffer
	if ret := C.rgw_write(fs.rgwFS, fh.handle, C.uint64_t(offset),
		C.size_t(length), &written, unsafe.Pointer(&buffer[0]),
		C.uint32_t(flags)); ret == 0 {
		return uint(written), nil
	} else {
		return uint(written), getError(ret)
	}
}

//    int rgw_read(rgw_fs *fs,
//                 rgw_file_handle *fh, uint64_t offset,
//                 size_t length, size_t *bytes_read, void *buffer,
//                 uint32_t flags)
func (fs *FS) Read(fh *FileHandle, offset, length uint64, buffer []byte,
	flags uint32) (bytes_read uint64, err error) {
	var cbytes_read C.size_t
	bufptr := unsafe.Pointer(&buffer[0])
	if ret := C.rgw_read(fs.rgwFS, fh.handle, C.uint64_t(offset),
		C.size_t(length), &cbytes_read, bufptr,
		C.uint32_t(flags)); ret == 0 {
		return uint64(cbytes_read), nil
	} else {
		return 0, getError(ret)
	}
}

// TODO: RGW_OPEN_FLAG_NONE
// int rgw_open(struct rgw_fs *rgw_fs,
//             struct rgw_file_handle *fh, uint32_t posix_flags, uint32_t flags)
func (fs *FS) Open(fh *FileHandle, posixFlags, flags uint32) error {
	if ret := C.rgw_open(fs.rgwFS, fh.handle, C.uint32_t(posixFlags),
		C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

//    int rgw_close(rgw_fs *fs, rgw_file_handle *fh,
//                  uint32_t flags)
func (fs *FS) Close(fh *FileHandle, flags uint32) error {
	if ret := C.rgw_close(fs.rgwFS, fh.handle, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

// Actually, do nothing
//    int rgw_fsync(rgw_fs *fs, rgw_file_handle *fh,
//                  uint32_t flags)
func (fs *FS) Fsync(fh *FileHandle, flags uint32) error {
	if ret := C.rgw_fsync(fs.rgwFS, fh.handle, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

// int rgw_commit(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
//               uint64_t offset, uint64_t length, uint32_t flags)
func (fs *FS) Commit(fh *FileHandle, offset, length uint64, flags uint32) error {
	if ret := C.rgw_commit(fs.rgwFS, fh.handle, C.uint64_t(offset),
		C.uint64_t(length), C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

// Actually, do nothing
// int rgw_truncate(rgw_fs *fs, rgw_file_handle *fh, uint64_t size, uint32_t flags)
func (fs *FS) Truncate(fh *FileHandle, size uint64, flags uint32) error {
	if ret := C.rgw_truncate(fs.rgwFS, fh.handle, C.uint64_t(size), C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

//    int rgw_unlink(rgw_fs *fs,
//                   rgw_file_handle *parent_fh, const char* path,
//                   uint32_t flags)
func (fs *FS) Unlink(parentHdl *FileHandle, path string, flags uint32) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	if ret := C.rgw_unlink(fs.rgwFS, parentHdl.handle, cpath, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

//    int rgw_rename(rgw_fs *fs,
//                   rgw_file_handle *olddir, const char* old_name,
//                   rgw_file_handle *newdir, const char* new_name,
//                   uint32_t flags)
func (fs *FS) Rename(oldDirHdl *FileHandle, oldName string,
	newDirHdl *FileHandle, newName string, flags uint32) error {
	cOldName := C.CString(oldName)
	defer C.free(unsafe.Pointer(cOldName))
	cNewName := C.CString(newName)
	defer C.free(unsafe.Pointer(cNewName))

	if ret := C.rgw_rename(fs.rgwFS, oldDirHdl.handle, cOldName,
		newDirHdl.handle, cNewName, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

//    int rgw_getattr(rgw_fs *fs,
//                    rgw_file_handle *fh, stat *st,
//                    uint32_t flags)
func (fs *FS) Fstat(fh *FileHandle, flags uint32) (*syscall.Stat_t, error) {
	var stat C.struct_stat
	if ret := C.rgw_getattr(fs.rgwFS, fh.handle, &stat, C.uint32_t(flags)); ret == 0 {
		st := syscall.Stat_t{
			Dev:     uint64(stat.st_dev),
			Ino:     uint64(stat.st_ino),
			Nlink:   uint64(stat.st_nlink),
			Mode:    uint32(stat.st_mode),
			Uid:     uint32(stat.st_uid),
			Gid:     uint32(stat.st_gid),
			Rdev:    uint64(stat.st_rdev),
			Size:    int64(stat.st_size),
			Blksize: int64(stat.st_blksize),
			Blocks:  int64(stat.st_blocks),
			// FIXME
			//	st.Atim = st.st_atime
			//	st.Mtim = st.st_mtime
			//	st.Ctim = st.st_ctime
		}
		return &st, nil
	} else {
		return nil, getError(ret)
	}
}
