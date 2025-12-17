#!/bin/bash

set -xe

sudo apt-get update
sudo apt-get install -y make

# We set this here, so it's the same between the copydb and sharding debian
# package, and between different arch builds
DATETIME=$(date -u +%Y%m%d)
COMMIT_SHA=$(git rev-parse --short HEAD)
PROJECT_BIN_TAG="-$COMMIT_SHA"

git status

if [[ "$1" != "--tagged-only" ]] ; then
	make copydb-deb DATETIME=$DATETIME COMMIT_SHA=$COMMIT_SHA
	make sharding-deb DATETIME=$DATETIME COMMIT_SHA=$COMMIT_SHA
fi

make copydb-deb DATETIME=$DATETIME COMMIT_SHA=$COMMIT_SHA PROJECT_BIN_TAG=$PROJECT_BIN_TAG
make sharding-deb DATETIME=$DATETIME COMMIT_SHA=$COMMIT_SHA PROJECT_BIN_TAG=$PROJECT_BIN_TAG

cd build
set +x

echo "Debian package built successfully as follows:"
ls -l ghostferry*

# Make sure the we didn't release a dirty build by accident
if ls | grep -q dirty; then
  echo "ERROR: source directory is not clean! refused to release. showing git status below:"
  git status
  exit 1
fi
