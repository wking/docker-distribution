package ipfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	_path "path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	shell "github.com/whyrusleeping/ipfs-shell"
)

const driverName = "ipfs"
const defaultAddr = "localhost:5001"
const defaultRoot = "/ipns/local/docker-registry"

func init() {
	factory.Register(driverName, &ipfsDriverFactory{})
}

// ipfsDriverFactory implements the factory.StorageDriverFactory interface
type ipfsDriverFactory struct{}

func (factory *ipfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters), nil
}

type driver struct {
	root  string
	shell *shell.Shell
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// IPFS daemon.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - addr
// - root
func FromParameters(parameters map[string]interface{}) *Driver {
	var addr = defaultAddr
	var root = defaultRoot
	if parameters != nil {
		addrInterface, ok := parameters["addr"]
		if ok {
			addr = fmt.Sprint(addrInterface)
		}
		rootInterface, ok := parameters["root"]
		if ok {
			root = fmt.Sprint(rootInterface)
		}
	}
	return New(addr, root)
}

// New constructs a new Driver with a given addr (address) and root (IPNS root)
func New(addr string, root string) *Driver {
	shell := shell.NewShell(addr)
	if strings.HasPrefix(root, "/ipns/local/") {
		info, err := shell.ID()
		if err != nil {
			return nil
		}
		root = strings.Replace(root, "local", info.ID, 1)
	}
	if !strings.HasPrefix(root, "/ipns/") {
		return nil
	}
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					shell: shell,
					root:  root,
				},
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.shell.Cat(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	log.Debugf("Got content %s: %s", path, content)

	return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	contentHash, err := d.shell.Add(bytes.NewReader(contents))
	if err != nil {
		return err
	}

	return d.addBubblePublish(ctx, d.fullPath(path), contentHash)
}

func (d *driver) addBubblePublish(ctx context.Context, path string, contentHash string) error {
	log.Debugf("Put content to %s as %s", path, contentHash)

	for {
		parentPath, childName := _path.Split(path)
		parentPath = strings.TrimRight(parentPath, "/")

		if parentPath == "/ipns" {
			err := d.shell.Publish(childName, contentHash)
			if err != nil {
				return err
			}
			log.Debugf("Published to %s: %s", childName, contentHash)
			return nil
		}

		var oldParentHash string
		parent, err := d.shell.FileList(parentPath)
		if err == nil {
			oldParentHash = parent.Hash
		} else {
			if !strings.HasPrefix(err.Error(), "no link named") {
				return err
			}
			emptyDirHash, err := d.shell.NewObject("unixfs-dir")
			if err != nil {
				return err
			}
			oldParentHash = emptyDirHash
		}

		tmpParentHash, err := d.shell.Patch(oldParentHash, "rm-link", childName)
		if err != nil {
			if err.Error() == "merkledag: not found" {
				tmpParentHash = oldParentHash
			} else {
				return err
			}
		}

		newParentHash, err := d.shell.Patch(tmpParentHash, "add-link", childName, contentHash)
		if err != nil {
			return err
		}

		log.Debugf("Update %s from %s to %s by adjusting %s", parentPath, oldParentHash, newParentHash, childName)

		path = parentPath
		contentHash = newParentHash
	}
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	reader, err := d.shell.Cat(d.fullPath(path))
	if err != nil {
		return nil, err
	}

	_, err = io.CopyN(ioutil.Discard, reader, offset)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(reader), nil
}

// WriteStream stores the contents of the provided io.Reader at a location
// designated by the given path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {
	fullPath := d.fullPath(path)

	oldReader, err := d.shell.Cat(fullPath)
	if err == nil {
		var buf bytes.Buffer

		nn, err = io.CopyN(&buf, oldReader, offset)
		if err != nil {
			return 0, err
		}

		_, err := io.Copy(&buf, reader)
		if err != nil {
			return 0, err
		}

		reader = &buf
	} else {
		if strings.HasPrefix(err.Error(), "no link named") {
			nn = 0
		} else {
			return 0, err
		}
	}

	contentHash, err := d.shell.Add(reader)
	if err != nil {
		return 0, err
	}

	log.Debugf("Wrote content (after %d) %s: %s", nn, path, contentHash)

	err = d.addBubblePublish(ctx, fullPath, contentHash)
	return nn, err
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	output, err := d.shell.FileList(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   output.Type == "Directory",
		ModTime: time.Time{},
	}

	if !fi.IsDir {
		fi.Size = int64(output.Size)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	output, err := d.shell.FileList(d.fullPath(path))
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	keys := make([]string, 0, len(output.Links))
	for _, link := range output.Links {
		keys = append(keys, _path.Join(path, link.Name))
	}

	return keys, nil
}

// Move moves an object stored at source to dest, removing the
// original object.
func (d *driver) Move(ctx context.Context, source string, dest string) error {
	sourceParentPath, sourceName := _path.Split(d.fullPath(source))

	sourceParent, err := d.shell.FileList(sourceParentPath)
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return storagedriver.PathNotFoundError{Path: source}
		}
		return err
	}

	var sourceHash string
	for _, link := range sourceParent.Links {
		if link.Name == sourceName {
			sourceHash = link.Hash
			break
		}
	}
	if sourceHash == "" {
		return storagedriver.PathNotFoundError{Path: source}
	}

	newSourceParentHash, err := d.shell.Patch(
		sourceParent.Hash, "rm-link", sourceName)
	if err != nil {
		if err.Error() == "merkledag: not found" {
			return storagedriver.PathNotFoundError{Path: source}
		} else {
			return err
		}
	}

	// TODO(wking): don't actually publish this, just get the hash back
	// for the next bubbler.  That would give us atomic moves.
	err = d.addBubblePublish(ctx, sourceParentPath, newSourceParentHash)
	if err != nil {
		return err
	}

	return d.addBubblePublish(ctx, d.fullPath(dest), sourceHash)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	parentPath, name := _path.Split(d.fullPath(path))

	parent, err := d.shell.FileList(parentPath)
	if err != nil {
		if strings.HasPrefix(err.Error(), "no link named") {
			return storagedriver.PathNotFoundError{Path: path}
		}
		return err
	}

	var hash string
	for _, link := range parent.Links {
		if link.Name == name {
			hash = link.Hash
			break
		}
	}
	if hash == "" {
		return storagedriver.PathNotFoundError{Path: path}
	}

	newParentHash, err := d.shell.Patch(parent.Hash, "rm-link", name)
	if err != nil {
		if err.Error() == "merkledag: not found" {
			return storagedriver.PathNotFoundError{Path: path}
		} else {
			return err
		}
	}

	return d.addBubblePublish(ctx, parentPath, newParentHash)
}

// URLFor returns a URL which may be used to retrieve the content
// stored at the given path.  It may return an UnsupportedMethodErr in
// certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod
}

// fullPath returns the absolute path of a key within the Driver's
// storage.
func (d *driver) fullPath(path string) string {
	return _path.Join(d.root, path)
}
