#!/bin/bash

set -e
current_branch=$(git rev-parse --abbrev-ref HEAD)
git checkout -b gh-pages
mkdocs build
git add site/
git commit -m 'update docs'
subtree_id=$(git subtree split --prefix site/ gh-pages)
git push upstream $subtree_id:gh-pages --force
git checkout $current_branch
git branch -D gh-pages
