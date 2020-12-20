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

type MountFlag uint32

const (
	MountFlagNone MountFlag = 0
)

// int rgw_mount(librgw_t rgw, const char *uid, const char *key,
//               const char *secret, rgw_fs **fs, uint32_t flags)
func (fs *FS) Mount(rgw *RGW, uid, key, secret string, flags MountFlag) error {
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

type UmountFlag uint32

const (
	UmountFlagNone UmountFlag = 0
)

// int rgw_umount(rgw_fs *fs, uint32_t flags)
func (fs *FS) Umount(flags UmountFlag) error {
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

type StatFSFlag uint32

const (
	StatFSFlagNone StatFSFlag = 0
)

//    int rgw_statfs(rgw_fs *fs, rgw_file_handle *parent_fh,
//                   rgw_statvfs *vfs_st, uint32_t flags)
func (fs *FS) StatFS(pFH *FileHandle, flags StatFSFlag) (*StatVFS, error) {
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

type ReadDirCallback interface {
	Callback(name string, st *syscall.Stat_t, mask AttrMask, flags uint32) bool
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
			Atim: syscall.Timespec{
				Sec:  int64(stat.st_atim.tv_sec),
				Nsec: int64(stat.st_atim.tv_nsec),
			},
			Mtim: syscall.Timespec{
				Sec:  int64(stat.st_mtim.tv_sec),
				Nsec: int64(stat.st_mtim.tv_nsec),
			},
			Ctim: syscall.Timespec{
				Sec:  int64(stat.st_ctim.tv_sec),
				Nsec: int64(stat.st_ctim.tv_nsec),
			},
		}
	}
	return cb.Callback(C.GoString(name), &st, AttrMask(mask), uint32(flags))
}

type ReadDirFlag uint32

const (
	ReadDirFlagNone   ReadDirFlag = 0
	ReadDirFlagDotDot ReadDirFlag = 1 // send dot names
)

// int rgw_readdir(struct rgw_fs *rgw_fs,
//                 struct rgw_file_handle *parent_fh, uint64_t *offset,
//                 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
//                 uint32_t flags)
func (fs *FS) ReadDir(parentHdl *FileHandle, cb ReadDirCallback, offset uint64, flags ReadDirFlag) (uint64, bool, error) {
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

type AttrMask uint32

const (
	AttrMode  AttrMask = 1
	AttrUid   AttrMask = 2
	AttrGid   AttrMask = 4
	AttrMtime AttrMask = 8
	AttrAtime AttrMask = 16
	AttrSize  AttrMask = 32
	AttrCtime AttrMask = 64
)

type LookupFlag uint32

const (
	LookupFlagNone   LookupFlag = 0
	LookupFlagCreate LookupFlag = 1
	LookupFlagRCB    LookupFlag = 2 // readdir callback hint
	LookupFlagDir    LookupFlag = 4
	LookupFlagFile   LookupFlag = 8
	LookupTypeFlags  LookupFlag = LookupFlagDir | LookupFlagFile
)

//    int rgw_lookup(rgw_fs *fs,
//                   rgw_file_handle *parent_fh, const char *path,
//                   rgw_file_handle **fh, stat* st, uint32_t st_mask,
//                   uint32_t flags)
func (fs *FS) Lookup(parentHdl *FileHandle, path string, stMask AttrMask,
	flags LookupFlag) (*FileHandle, *syscall.Stat_t, error) {
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
			Atim: syscall.Timespec{
				Sec:  int64(stat.st_atim.tv_sec),
				Nsec: int64(stat.st_atim.tv_nsec),
			},
			Mtim: syscall.Timespec{
				Sec:  int64(stat.st_mtim.tv_sec),
				Nsec: int64(stat.st_mtim.tv_nsec),
			},
			Ctim: syscall.Timespec{
				Sec:  int64(stat.st_ctim.tv_sec),
				Nsec: int64(stat.st_ctim.tv_nsec),
			},
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

type CreateFlag uint32

const (
	CreateFlagNone CreateFlag = 0
)

//    int rgw_create(rgw_fs *fs, rgw_file_handle *parent_fh,
//                   const char *name, stat *st, uint32_t mask,
//                   rgw_file_handle **fh, uint32_t posix_flags,
//                   uint32_t flags)
func (fs *FS) Create(parentHdl *FileHandle, name string, mask AttrMask,
	posixFlags uint32, flags CreateFlag) (
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
			Atim: syscall.Timespec{
				Sec:  int64(stat.st_atim.tv_sec),
				Nsec: int64(stat.st_atim.tv_nsec),
			},
			Mtim: syscall.Timespec{
				Sec:  int64(stat.st_mtim.tv_sec),
				Nsec: int64(stat.st_mtim.tv_nsec),
			},
			Ctim: syscall.Timespec{
				Sec:  int64(stat.st_ctim.tv_sec),
				Nsec: int64(stat.st_ctim.tv_nsec),
			},
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

type MkdirFlag uint32

const (
	MkdirFlagNone MkdirFlag = 0
)

//    int rgw_mkdir(rgw_fs *fs,
//                  rgw_file_handle *parent_fh,
//                  const char *name, stat *st, uint32_t mask,
//                  rgw_file_handle **fh, uint32_t flags)
//
func (fs *FS) Mkdir(parentHdl *FileHandle, name string, mask AttrMask, flags MkdirFlag) (
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
			Atim: syscall.Timespec{
				Sec:  int64(stat.st_atim.tv_sec),
				Nsec: int64(stat.st_atim.tv_nsec),
			},
			Mtim: syscall.Timespec{
				Sec:  int64(stat.st_mtim.tv_sec),
				Nsec: int64(stat.st_mtim.tv_nsec),
			},
			Ctim: syscall.Timespec{
				Sec:  int64(stat.st_ctim.tv_sec),
				Nsec: int64(stat.st_ctim.tv_nsec),
			},
		}
		return fh, &st, nil
	} else {
		return nil, nil, getError(ret)
	}
}

type WriteFlag uint32

const (
	WriteFlagNone WriteFlag = 0
)

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

type ReadFlag uint32

const (
	ReadFlagNone ReadFlag = 0
)

//    int rgw_read(rgw_fs *fs,
//                 rgw_file_handle *fh, uint64_t offset,
//                 size_t length, size_t *bytes_read, void *buffer,
//                 uint32_t flags)
func (fs *FS) Read(fh *FileHandle, offset, length uint64, buffer []byte,
	flags ReadFlag) (bytes_read uint64, err error) {
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

type OpenFlag uint32

const (
	OpenFlagNone      OpenFlag = 0
	OpenFlagCreate    OpenFlag = 1
	OpenFlagV3        OpenFlag = 2 // ops have v3 semantics
	OpenFlagStateless OpenFlag = 2 // alias it
)

// int rgw_open(struct rgw_fs *rgw_fs,
//             struct rgw_file_handle *fh, uint32_t posix_flags, uint32_t flags)
func (fs *FS) Open(fh *FileHandle, posixFlags uint32, flags OpenFlag) error {
	if ret := C.rgw_open(fs.rgwFS, fh.handle, C.uint32_t(posixFlags),
		C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type CloseFlag uint32

const (
	CloseFlagNone CloseFlag = 0
	CloseFlagRele CloseFlag = 1
)

//    int rgw_close(rgw_fs *fs, rgw_file_handle *fh,
//                  uint32_t flags)
func (fs *FS) Close(fh *FileHandle, flags CloseFlag) error {
	if ret := C.rgw_close(fs.rgwFS, fh.handle, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type FsyncFlag uint32

const (
	FsyncFlagNone FsyncFlag = 0
)

// Actually, do nothing
//    int rgw_fsync(rgw_fs *fs, rgw_file_handle *fh,
//                  uint32_t flags)
func (fs *FS) Fsync(fh *FileHandle, flags FsyncFlag) error {
	if ret := C.rgw_fsync(fs.rgwFS, fh.handle, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type CommitFlag uint32

const (
	CommitFlagNone CommitFlag = 0
)

// int rgw_commit(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
//               uint64_t offset, uint64_t length, uint32_t flags)
func (fs *FS) Commit(fh *FileHandle, offset, length uint64, flags CommitFlag) error {
	if ret := C.rgw_commit(fs.rgwFS, fh.handle, C.uint64_t(offset),
		C.uint64_t(length), C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type TruncFlag uint32

const (
	TruncFlagNone TruncFlag = 0
)

// Actually, do nothing
// int rgw_truncate(rgw_fs *fs, rgw_file_handle *fh, uint64_t size, uint32_t flags)
func (fs *FS) Truncate(fh *FileHandle, size uint64, flags TruncFlag) error {
	if ret := C.rgw_truncate(fs.rgwFS, fh.handle, C.uint64_t(size), C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type UnlinkFlag uint32

const (
	UnlinkFlagNone UnlinkFlag = 0
)

//    int rgw_unlink(rgw_fs *fs,
//                   rgw_file_handle *parent_fh, const char* path,
//                   uint32_t flags)
func (fs *FS) Unlink(parentHdl *FileHandle, path string, flags UnlinkFlag) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	if ret := C.rgw_unlink(fs.rgwFS, parentHdl.handle, cpath, C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}

type RenameFlag uint32

const (
	RenameFlagNone RenameFlag = 0
)

//    int rgw_rename(rgw_fs *fs,
//                   rgw_file_handle *olddir, const char* old_name,
//                   rgw_file_handle *newdir, const char* new_name,
//                   uint32_t flags)
func (fs *FS) Rename(oldDirHdl *FileHandle, oldName string,
	newDirHdl *FileHandle, newName string, flags RenameFlag) error {
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

type GetAttrFlag uint32

const (
	GetAttrFlagNone GetAttrFlag = 0
)

//    int rgw_getattr(rgw_fs *fs,
//                    rgw_file_handle *fh, stat *st,
//                    uint32_t flags)
func (fs *FS) GetAttr(fh *FileHandle, flags GetAttrFlag) (*syscall.Stat_t, error) {
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
			Atim: syscall.Timespec{
				Sec:  int64(stat.st_atim.tv_sec),
				Nsec: int64(stat.st_atim.tv_nsec),
			},
			Mtim: syscall.Timespec{
				Sec:  int64(stat.st_mtim.tv_sec),
				Nsec: int64(stat.st_mtim.tv_nsec),
			},
			Ctim: syscall.Timespec{
				Sec:  int64(stat.st_ctim.tv_sec),
				Nsec: int64(stat.st_ctim.tv_nsec),
			},
		}
		return &st, nil
	} else {
		return nil, getError(ret)
	}
}

type SetAttrFlag uint32

const (
	SetAttrFlagNone SetAttrFlag = 0
)

//    int rgw_setattr(rgw_fs *fs, rgw_file_handle *fh, stat *st,
//                    uint32_t mask, uint32_t flags)
func (fs *FS) SetAttr(fh *FileHandle, stat *syscall.Stat_t, mask AttrMask, flags SetAttrFlag) error {
	st := C.struct_stat{
		st_dev:     C.uint64_t(stat.Dev),
		st_ino:     C.uint64_t(stat.Ino),
		st_nlink:   C.uint64_t(stat.Nlink),
		st_mode:    C.uint32_t(stat.Mode),
		st_uid:     C.uint32_t(stat.Uid),
		st_gid:     C.uint32_t(stat.Gid),
		st_rdev:    C.uint64_t(stat.Rdev),
		st_size:    C.int64_t(stat.Size),
		st_blksize: C.int64_t(stat.Blksize),
		st_blocks:  C.int64_t(stat.Blocks),
		st_atim: C.struct_timespec{
			tv_sec:  C.long(stat.Atim.Sec),
			tv_nsec: C.long(stat.Atim.Nsec),
		},
		st_mtim: C.struct_timespec{
			tv_sec:  C.long(stat.Mtim.Sec),
			tv_nsec: C.long(stat.Mtim.Nsec),
		},
		st_ctim: C.struct_timespec{
			tv_sec:  C.long(stat.Ctim.Sec),
			tv_nsec: C.long(stat.Ctim.Nsec),
		},
	}

	if ret := C.rgw_setattr(fs.rgwFS, fh.handle, &st, C.uint32_t(mask),
		C.uint32_t(flags)); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}
}
