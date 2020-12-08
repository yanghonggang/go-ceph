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
	//	"syscall"
	"fmt"
	"unsafe"
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
	ret := C.rgw_mount(*rgw.libRGW, C.CString(uid), C.CString(key),
		C.CString(secret), &fs.rgwFS, C.uint(flags))
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

//export goCommonReadDirCallback
func goCommonReadDirCallback(name *C.char) bool {
	fmt.Println(C.GoString(name))
	return true
}

// int rgw_readdir(struct rgw_fs *rgw_fs,
//                 struct rgw_file_handle *parent_fh, uint64_t *offset,
//                 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
//                 uint32_t flags)
func (fs *FS) ReadDir(parentHdl *FileHandle, offset uint64, flags uint32) (uint64, bool, error) {
	coffset := C.uint64_t(offset)
	var eof C.bool = false
	if ret := C.rgw_readdir(fs.rgwFS, parentHdl.handle, &coffset, C.rgw_readdir_cb(C.common_readdir_cb),
		unsafe.Pointer(nil), &eof, C.uint(flags)); ret != 0 {
		return 0, false, getError(ret)
	} else {
		next := uint64(coffset)
		return next, bool(eof), getError(ret)
	}
}
