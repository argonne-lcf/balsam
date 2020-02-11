# Developer Guidelines

1. Install development code with:
    ```
    pip install -e .[dev,server]
    ```
2. Install pre-commit hooks:
    ```
    pre-commit install
    ```

On commit, code is auto-formatted with `black` and linted with `flake8`.  Linting errors will cause commit to fail.

# Travis CI

- Style and linting is enforced on the repo with TravisCI pre-commit
- On commit, tests run under coverage