package rgw

/*
#cgo LDFLAGS: -lrgw
#include <stdlib.h>
#include <rados/librgw.h>
#include <rados/rgw_file.h>
*/
import "C"

import (
	"unsafe"
)

// typedef void* librgw_t;
type LibRGW struct {
	rgw *C.librgw_t
}

// int librgw_create(librgw_t *rgw, int argc, char **argv)
func createRGW(argc C.int, argv **C.char) (*LibRGW, error) {
	libRGW := &LibRGW{}
	if ret := C.librgw_create(libRGW.rgw, argc, argv); ret == 0 {
		return libRGW, nil
	} else {
		return nil, getError(ret)
	}
}

func CreateRGW(argv []string) (*LibRGW, error) {
	cargv := make([]*C.char, len(argv))
	for i := range argv {
		cargv[i] = C.CString(argv[i])
		defer C.free(unsafe.Pointer(cargv[i]))
	}

	return createRGW(C.int(len(cargv)), &cargv[0])
}

// void librgw_shutdown(librgw_t rgw)
func ShutdownRGW(libRGW *LibRGW) {
	C.librgw_shutdown(*libRGW.rgw)
}

// FS exports ceph's rgw_fs from include/rados/rgw_file.h
type FS struct {
	rgwFS *C.struct_rgw_fs
}

// int rgw_mount(librgw_t rgw, const char *uid, const char *key,
//               const char *secret, rgw_fs **fs, uint32_t flags)
func (fs *FS) Mount(libRGW *LibRGW, uid, key, secret *C.char, flags C.uint) error {
	ret := C.rgw_mount(*libRGW.rgw, uid, key, secret, &fs.rgwFS, flags)
	if ret != 0 {
		return getError(ret)
	}

	return nil
}

// int rgw_umount(rgw_fs *fs, uint32_t flags)
func (fs *FS) Umount(flags C.uint) error {
	if ret := C.rgw_umount(fs.rgwFS, flags); ret == 0 {
		return nil
	} else {
		return getError(ret)
	}

}
