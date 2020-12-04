package rgw

import (
	"fmt"
	"testing"

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

func TestMountUmount(t *testing.T) {
	rgw, err := CreateRGW([]string{"-c /etc/ceph/ceph.conf", "--name client.rgw.bjlt03-e57"})
	assert.NoError(t, err)
	assert.NotNil(t, rgw)

	fs := FS{}
	err = fs.Mount(rgw, "test", "ak", "sk", 0)
	assert.NoError(t, err)

	statVFS, err := fs.StatFS(fs.GetRootFileHandle(), 0)
	assert.NotNil(t, statVFS)
	assert.NoError(t, err)
	fmt.Println(">>>> statVFS.Blocks ", statVFS.Blocks)

	err = fs.Umount(0)
	assert.NoError(t, err)

	ShutdownRGW(rgw)
}
