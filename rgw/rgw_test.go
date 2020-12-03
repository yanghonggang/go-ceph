package rgw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateRGW(t *testing.T) {
	libRGW, err := CreateRGW([]string{"-c /etc/ceph/ceph.conf", "--name client.rgw.bjlt03-e57"})
	assert.NoError(t, err)
	assert.NotNil(t, libRGW)
}
