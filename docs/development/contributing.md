# Developer Guidelines

Before commiting any changes to the repository, install the development code into a 
virtual environment and install the **pre-commit** hooks as follows:

```
pip install -e .[dev,server]
pre-commit install
```

On commit, code will be auto-formatted with `black` and linted with `flake8`.
Linting errors will cause the commit to fail and point to errors.

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

## Creating diagrams in markdown
Refer to [mermaid.js](https://mermaid-js.github.io/mermaid/#/) for examples on graphs, flowcharts, sequence diagrams, class diagrams, state diagrams, etc...

```mermaid
graph TD
    A[Hard] -->|Text| B(Round)
    B --> C{Decision}
    C -->|One| D[Result 1]
    C -->|Two| E[Result 2]
```

```mermaid
stateDiagram-v2
    created: Created
    awaiting_parents: Awaiting Parents
    ready: Ready
    staged_in: Staged In
    preprocessed: Preprocessed
    restart_ready: Restart Ready
    running: Running
    run_done: Run Done
    postprocessed: Postprocessed
    staged_out: Staged Out
    finished: Job Finished
    run_error: Run Error
    run_timeout: Run Timeout
    failed: Failed

    created --> ready: No parents
    created --> awaiting_parents: Pending dependencies
    awaiting_parents --> ready: Dependencies finished
    ready --> staged_in: Transfer external data in
    staged_in --> preprocessed: Run preprocess script
    preprocessed --> running: Launch job

    running --> run_done: Return code 0
    running --> run_error: Nonzero return
    running --> run_timeout: Early termination

    run_timeout --> restart_ready: Auto retry
    run_error --> restart_ready: Run error handler
    run_error --> failed: No error handler
    restart_ready --> running: Launch job

    run_done --> postprocessed: Run postprocess script
    postprocessed --> staged_out: Transfer data out
    staged_out --> finished: Job Finished
```
