package rgw

/*
#cgo LDFLAGS: -lrgw
#include <rados/librgw.h>
#include <rados/rgw_file.h>
*/
import "C"

import (
	"unsafe"
)

// FS exports ceph's rgw_fs from include/rados/rgw_file.h
type FS struct {
	fs *C.struct_rgw_fs
}

// int rgw_mount(librgw_t rgw, const char *uid, const char *key,
//               const char *secret, rgw_fs **fs, uint32_t flags)
func Mount(rgw unsafe.Pointer, uid, key, secret *C.char, flags C.uint) (*FS, error) {
	fs := &FS{}
	ret := C.rgw_mount(C.librgw_t(rgw), uid, key, secret, &fs.fs, flags)
	if ret != 0 {
		return nil, getError(ret)
	}
	return fs, nil
}
