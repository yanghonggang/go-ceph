package rgw

// example:
// $ export GO_CEPH_TEST_CEPH_CONF="/etc/ceph/ceph.conf"
// $ export GO_CEPH_TEST_CLIENT_NAME="client.rgw.bjlt03-e57"
// $ export GO_CEPH_TEST_S3_USER="test"
// $ export GO_CEPH_TEST_S3_AK="ak"
// $ export GO_CEPH_TEST_S3_SK="sk"
// $ go test -v
import (
	"fmt"
	"math/rand"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	rgw *RGW = nil

	cephConf   string = os.Getenv("GO_CEPH_TEST_CEPH_CONF")
	clientName string = os.Getenv("GO_CEPH_TEST_CLIENT_NAME")
	s3User     string = os.Getenv("GO_CEPH_TEST_S3_USER")
	ak         string = os.Getenv("GO_CEPH_TEST_S3_AK")
	sk         string = os.Getenv("GO_CEPH_TEST_S3_SK")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestCreateRGW(t *testing.T) {
	confStr := "-c " + cephConf
	nameStr := "--name " + clientName
	rgwLocal, err := CreateRGW([]string{confStr, nameStr})
	assert.NoError(t, err)
	assert.NotNil(t, rgwLocal)

	rgw = rgwLocal
}

func TestMountUmount(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)
}

func TestVersion(t *testing.T) {
	fs := FS{}
	fs.Version()
}

func TestStatFS(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	statVFS, err := fs.StatFS(fs.GetRootFileHandle(), 0)
	assert.NotNil(t, statVFS)
	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)
}

type ReadDirCallbackDump struct{}

func (cb *ReadDirCallbackDump) Callback(name string, st *syscall.Stat_t, mask, flags uint32) bool {
	fmt.Printf("name: %v\n", name)
	return true
}

func TestMkdir(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	bName := fmt.Sprintf("mybucket-%v", rand.Int63())
	fhB, stB, err := fs.Mkdir(fs.GetRootFileHandle(), bName, 0, 0)
	assert.NotNil(t, fhB)
	assert.NotNil(t, stB)
	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)
}

func TestReadDir(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	cb := &ReadDirCallbackDump{}
	fs.ReadDir(fs.GetRootFileHandle(), cb, 0, 0)

	err = fs.Umount(0)
	assert.NoError(t, err)
}

func TestCreateUnlink(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	bName := fmt.Sprintf("mybucket-%v", rand.Int63())
	fhB, stB, err := fs.Mkdir(fs.GetRootFileHandle(), bName, 0, 0)
	assert.NotNil(t, fhB)
	assert.NotNil(t, stB)
	assert.NoError(t, err)

	objName := fmt.Sprintf("obj-%v", rand.Int63())
	fhObj, _, err := fs.Create(fhB, objName, 0, 0, 0)
	assert.NotNil(t, fhObj)
	assert.NoError(t, err)

	err = fs.Unlink(fhB, objName, 0)
	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)
}

/*
func Test2(t *testing.T) {
	fs := FS{}
	err := fs.Mount(rgw, s3User, ak, sk, 0)
	assert.NoError(t, err)

	major, minor, extra := fs.Version()
	fmt.Printf("major: %v, minor: %v, extra: %v\n", major, minor, extra)

	statVFS, err := fs.StatFS(fs.GetRootFileHandle(), 0)
	assert.NotNil(t, statVFS)
	assert.NoError(t, err)
	fmt.Println(">>>> statVFS.Blocks ", statVFS.Blocks)

	cb := &ReadDirCallbackDump{}
	fs.ReadDir(fs.GetRootFileHandle(), cb, 0, 0)

	bucketName := "yhgtest"
	fh, st, err := fs.Lookup(fs.GetRootFileHandle(), bucketName, 0, 0)
	assert.NotNil(t, fh)
	assert.NoError(t, err)
	fmt.Printf("st: %v\n", st)
	//fs.ReadDir(fh, cb, 0, 0)


	dirName := fmt.Sprintf("mydirP%vP%v", rand.Int63(), rand.Int63())
	fhDir, stDir, err := fs.Mkdir(fh, dirName, 0, 0)
	assert.NotNil(t, fhDir)
	assert.NoError(t, err)
	fmt.Printf("stat of %v: %v\n", dirName, stDir)
	//	fs.ReadDir(fh, cb, 0, 0)

	objName := fmt.Sprintf("hahaP%vP%v", rand.Int63(), rand.Int63())
	fhObj, stObj, err := fs.Create(fhDir, objName, 0, 0, 0)
	assert.NotNil(t, fhObj)
	assert.NoError(t, err)
	fmt.Printf("stat of %v: %v\n", objName, stObj)
	//fs.ReadDir(fhDir, cb, 0, 0)

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
	fmt.Printf("read back after close: %v, err %v, bytes %v\n", string(buffer2), err, bytes)

	err = fs.Truncate(fhObj, 2, 0)
	assert.NotNil(t, bytes)

	bytes, err = fs.Read(fhObj, 0, uint64(len(buffer)), buffer2, 0)
	assert.NotNil(t, bytes)
	assert.NoError(t, err)
	fmt.Printf("read back after truncate to 2 bytes: %v, err %v, bytes %v\n", string(buffer2), err, bytes)

	//fs.ReadDir(fhDir, cb, 0, 0)

	fhObj2, _, err := fs.Create(fh, "obj111", 0, 0, 0)
	assert.NotNil(t, fhObj2)
	assert.NoError(t, err)

	err = fs.Close(fhObj2, 0)
	assert.NoError(t, err)

	err = fs.Unlink(fhDir, objName, 0)
	assert.NoError(t, err)

	st2 := &syscall.Stat_t{}
	err = fs.SetAttr(fhObj2, st2, 0, 0)
	assert.NoError(t, err)

	st3, err := fs.GetAttr(fhObj2, 0)
	assert.NoError(t, err)
	fmt.Printf("object2 stat: %v", st3)

	err = fs.Rename(fh, "obj111", fhDir, "obj222", 0)
	assert.NoError(t, err)

	err = fs.Umount(0)
	assert.NoError(t, err)
}
*/

func TestShutdownRGW(t *testing.T) {
	ShutdownRGW(rgw)
}
