#!/bin/bash

set -xe

make copydb-deb IGNORE_DIRTY_TREE=1
make sharding-deb IGNORE_DIRTY_TREE=1
