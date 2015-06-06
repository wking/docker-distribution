<!--GITHUB
page_title: IPFS storage driver
page_description: Explains how to use the IPFS storage driver
page_keywords: registry, service, driver, images, storage, ipfs
IGNORES-->

# IPFS storage driver

An implementation of the `storagedriver.StorageDriver` interface which
uses [IPFS][].

## Parameters

`addr`: (optional) The address for an IPFS API server.  Defaults to
  `localhost:5001`.

`root`: (optional) The IPFS name/path under which the registry should
  be published.  The special name `local` can be used as a synonym for
  the local IPFS API server's ID.  Defaults to
  `/ipns/local/docker-registry`.

[IPFS]: http://ipfs.io/
