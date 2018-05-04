#!/bin/bash

set -xe

REPO=git@github.com:Shopify/ghostferry.git
REV=${CIRCLE_BRANCH:-$CIRCLE_TAG}

pushd ~
mkdir gh-pages
pushd gh-pages
git config --global user.email "circleci@shopify.com"
git config --global user.name "CircleCI Docs Deployer"
git init
git remote add origin $REPO
git fetch origin
git checkout gh-pages
popd
popd

cd docs
make html
cp -r build/html/* ~/gh-pages/$REV
cd ~/gh-pages
git add .
git commit -am "auto updated documentations"
git push origin gh-pages
