#!/bin/bash

set -xe

sudo apt-get update
sudo apt-get install -y python3-pip

python3 -m venv ./venv
source venv/bin/activate

pip3 install sphinx==8.1.3
cd docs
make html

cd build
git clone --depth 1 https://github.com/Shopify/ghostferry.git -b gh-pages ghostferry-pages
current_branch=${GITHUB_REF#refs/heads/}
cp -ar html/. ghostferry-pages/${current_branch}
cd ghostferry-pages

echo "<html>" > index.html
echo "  <head>" >> index.html
echo "    <title>Ghostferry Documentations</title>" >> index.html
echo "  </head>" >> index.html
echo "  <body>" >> index.html
echo "    <h3>Ghostferry Documentation Version Selector</h3>" >> index.html
echo "    <ul>" >> index.html
for d in */; do
echo "      <li><a href=\"${d}index.html\">${d}</a></li>" >> index.html
done
echo "    </ul>" >> index.html
echo "  </body>" >> index.html
echo "</html>" >> index.html

git status

cd ../../..
ls -la docs/build/ghostferry-pages
rm .git -rf
