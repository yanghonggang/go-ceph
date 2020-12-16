package rgw

import (
	"fmt"
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

/*
func TestCreateRGW(t *testing.T) {
	rgw, err := CreateRGW([]string{"-c /etc/ceph/ceph.conf", "--name client.rgw.bjlt03-e57"})
	assert.NoError(t, err)
	assert.NotNil(t, rgw)
	ShutdownRGW(rgw)
}
*/

type ReadDirCallbackDump struct {
}

func (cb *ReadDirCallbackDump) Callback(name string, st *syscall.Stat_t, mask, flags uint32) bool {
	fmt.Printf("name: %v, stat: %v\n", name, *st)
	return true
}

func TestMountUmount(t *testing.T) {
	rgw, err := CreateRGW([]string{"-c /etc/ceph/ceph.conf", "--name client.rgw.bjlt03-e57"})
	assert.NoError(t, err)
	assert.NotNil(t, rgw)

	fs := FS{}
	err = fs.Mount(rgw, "test", "ak", "sk", 0)
	assert.NoError(t, err)

	major, minor, extra := fs.Version()
	fmt.Printf("major: %v, minor: %v, extra: %v\n", major, minor, extra)

	statVFS, err := fs.StatFS(fs.GetRootFileHandle(), 0)
	assert.NotNil(t, statVFS)
	assert.NoError(t, err)
	fmt.Println(">>>> statVFS.Blocks ", statVFS.Blocks)

	cb := &ReadDirCallbackDump{}
	fs.ReadDir(fs.GetRootFileHandle(), cb, 0, 0)

	fh, st, err := fs.Lookup(fs.GetRootFileHandle(), "yhgtest", 0, 0)
	assert.NotNil(t, fh)
	assert.NoError(t, err)
	fmt.Printf("st: %v\n", st)
	fs.ReadDir(fh, cb, 0, 0)

	rand.Seed(time.Now().UnixNano())

	dirName := fmt.Sprintf("mydir-%v-%v", rand.Int63(), rand.Int63())
	fhDir, stDir, err := fs.Mkdir(fh, dirName, 0, 0)
	assert.NotNil(t, fhDir)
	assert.NoError(t, err)
	fmt.Printf("stat of %v: %v\n", dirName, stDir)
	fs.ReadDir(fh, cb, 0, 0)

	objName := fmt.Sprintf("haha-%v-%v", rand.Int63(), rand.Int63())
	fhObj, stObj, err := fs.Create(fhDir, objName, 0, 0, 0)
	assert.NotNil(t, fhObj)
	assert.NoError(t, err)
	fmt.Printf("stat of %v: %v\n", objName, stObj)
	fs.ReadDir(fhDir, cb, 0, 0)

	err = fs.Open(fhObj, 0, 0)
	assert.NoError(t, err)

	buffer := []byte{'h', 'e', 'l', 'l', 'o'}
	written, err := fs.Write(fhObj, buffer, 0, uint64(len(buffer)), 0)
	fmt.Printf("written %v, err %v\n", written, err)

	// FIXME: it seems that buffer is not committed
	err = fs.Commit(fhObj, 0, uint64(len(buffer)), 0)
	assert.NoError(t, err)

	buffer2 := make([]byte, len(buffer))
	bytes, err := fs.Read(fhObj, 0, uint64(len(buffer)), buffer2, 0)
	assert.NotNil(t, bytes)
	assert.NoError(t, err)
	fmt.Printf("read back: %v, err %v, bytes %v\n", buffer2, err, bytes)

	err = fs.Close(fhObj, 0)
	assert.NoError(t, err)

	bytes, err = fs.Read(fhObj, 0, uint64(len(buffer)), buffer2, 0)
	assert.NotNil(t, bytes)
	assert.NoError(t, err)
	fmt.Printf("read back: %v, err %v, bytes %v\n", string(buffer2), err, bytes)

	err = fs.Truncate(fhObj, 2, 0)
	assert.NotNil(t, bytes)

	bytes, err = fs.Read(fhObj, 0, uint64(len(buffer)), buffer2, 0)
	assert.NotNil(t, bytes)
	assert.NoError(t, err)
	fmt.Printf("read back after truncate to 2 bytes: %v, err %v, bytes %v\n", string(buffer2), err, bytes)

	fs.ReadDir(fhDir, cb, 0, 0)

	/// FIXME: unlink error
	//	fullName := fmt.Sprintf("%s/%s", dirName, objName)
	//	fmt.Println("fullName ", fullName)
	//	err = fs.Unlink(fh, fullName, 0)
	//	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)

	ShutdownRGW(rgw)
}
