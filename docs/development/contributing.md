# Developer Guidelines

## Porting to new HPC systems

To port Balsam to a new system, a developer should only need to 
implement the following platform interfaces:

- `platform/app_run`: Add AppRun subclass and list it in the __init__.py
- `platform/compute_node`: Same for ComputeNode
- `platform/scheduler`: Same for SchedulerInterface

Then create a new default configuration folder for the Site under `balsam/config/defaults`.  This isn't strictly necessary (users can write their own config files) but it makes it very convenient for others to quickly spin up a Site with the interfaces you wrote.  

You will need the following inside the default Site configuration directory:

- `apps/__init__.py` (and other default apps therein)
- `settings.yml` (Referencing the platform interfaces added above)
- `job-template.sh`

## Developer Installation

```bash
# Use a Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with deployment/development dependencies:
make install-dev

# Set up pre-commit linting hooks:
pre-commit install
```

On commit, code will be auto-formatted with `isort` and `black` and linted with `flake8`.
Linting errors will cause the commit to fail and point to errors.

Contributors may also run the following to re-format, lint, type-check, and test the code:

```bash
$ make format
$ make all
```

Please run these steps before making pull requests.


## Creating diagrams in markdown
Refer to [mermaid.js](https://mermaid-js.github.io/mermaid/#/) for examples on graphs, flowcharts, sequence diagrams, class diagrams, state diagrams, etc...

```mermaid
graph TD
    A[Hard] -->|Text| B(Round)
    B --> C{Decision}
    C -->|One| D[Result 1]
    C -->|Two| E[Result 2]
```

