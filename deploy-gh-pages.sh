#!/bin/bash

set -e
current_branch=$(git rev-parse --abbrev-ref HEAD)

# Create gh-pages local branch, build the docs, add new docs commit
git checkout -b gh-pages
mkdocs build
git add site/
git commit -m 'update docs'

# Force-push the built docs (site/ directory) to gh-pages branch upstream
subtree_id=$(git subtree split --prefix site/ gh-pages)
git push upstream $subtree_id:gh-pages --force

# Come back to the current branch and remove the temp branch
git checkout $current_branch
git branch -D gh-pages
