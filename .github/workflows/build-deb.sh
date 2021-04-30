#!/bin/bash

set -xe

sudo apt-get update
sudo apt-get install -y make

# We set this here, so it's the same between the copydb and sharding debian
# package.
DATETIME=$(date -u +%Y%m%d%H%M%S)

git status

make copydb-deb DATETIME=${DATETIME}
make sharding-deb DATETIME=${DATETIME}

cd build
sha256sum *.deb > ghostferry-$GITHUB_SHA.sha256sum

set +x

echo "Debian package built successfully as follows:"
ls -l ghostferry*

echo "sha256sum:"
cat ghostferry-$GITHUB_SHA.sha256sum

# Make sure the we didn't release a dirty build by accident
if ls | grep -q dirty; then
  echo "ERROR: source directory is not clean! refused to release. showing git status below:"
  git status
  exit 1
fi
