# User installation

Balsam users should simply add Balsam to their environment with `pip`. 
```
git clone https://github.com/balsam-alcf/balsam.git
cd balsam
git checkout develop

# Set up Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with flexible (unpinned) dependencies:
pip install -e .
```

# Developer/server-side installation

For Balsam development and server deployments, there are some additional
requirements.  Use `make install-dev` to install Balsam with the necessary dependencies.  Direct server dependencies (e.g. FastAPI) are pinned to help with reproducible deployments.

```
git clone https://github.com/balsam-alcf/balsam.git
cd balsam
git checkout develop

# Set up Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with pinned deployment and dev dependencies:
make install-dev

# Set up pre-commit linting hooks:
pre-commit install
```

## To view the docs in your browser:

Navigate to top-level balsam directory (where `mkdocs.yml` is located) and run:
```
mkdocs serve
```

Follow the link to the documentation. Docs are markdown files in the `balsam/docs` subdirectory and can be edited 
on-the-fly.  The changes will auto-refresh in the browser window.
