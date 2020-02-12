[![codecov](https://codecov.io/gh/balsam-alcf/balsam/branch/master/graph/badge.svg)](https://codecov.io/gh/balsam-alcf/balsam)

[![Build Status](https://travis-ci.com/masalim2/balsam2.svg?token=yZnPndwLys7xKxFNNhSH&branch=develop)](https://travis-ci.com/masalim2/balsam2)

# Developer Guidelines

1. Install development code with:
    ```bash
    pip install -e .[dev,server]
    ```
2. Install pre-commit hooks:
    ```bash
    pre-commit install
    ```

On commit, code is auto-formatted with `black` and linted with `flake8`.  Linting errors will cause commit to fail.

## Travis CI
- Pre commit: black & flake8
- Run tests, generate coverage report

## ReadTheDocs
- commit hook to build MkDocs

## Testing

Test the DRF backend with PyTest:
```bash
$ pytest --cov=balsam/server balsam/server
```

Generate HTML report locally:
```bash
$ coverage html
$ open htmlcov/index.html
```
