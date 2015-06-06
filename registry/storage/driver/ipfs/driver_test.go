package ipfs

import (
	"io/ioutil"
	"os"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func init() {
	addr := os.Getenv("REGISTRY_STORAGE_IPFS_ADDR")
	root := os.Getenv("REGISTRY_STORAGE_IPFS_ROOT")

	testsuites.RegisterInProcessSuite(func() (storagedriver.StorageDriver, error) {
		return New(addr, root), nil
	}, testsuites.NeverSkip)

	// BUG(stevvooe): IPC is broken so we're disabling for now. Will revisit later.
	// testsuites.RegisterIPCSuite(driverName, map[string]string{"rootdirectory": root}, testsuites.NeverSkip)
}
