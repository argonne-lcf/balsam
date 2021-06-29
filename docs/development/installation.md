# Developer and service installations

For Balsam development and server deployments, there are some additional
requirements.  Use `make install-dev` to install Balsam with the necessary dependencies.  Direct server dependencies (e.g. FastAPI) are pinned to help with reproducible deployments.

```
git clone https://github.com/argonne-lcf/balsam.git
cd balsam

# Set up Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with pinned deployment and dev dependencies:
make install-dev

# Set up pre-commit linting hooks:
pre-commit install
```