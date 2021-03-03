# Integration Testing Balsam on new HPC platforms

## Registering a new test platform

In `tests/test_platform.py`:

Add a supported platform string to the `PLATFORMS` set:

```py3
PLATFORMS: Set[str] = {"alcf_theta", "alcf_thetagpu", "alcf_cooley", "generic"}
``` 

`tests/test_platform.py` also references some environment variables that any test runner (CI or manual) needs to set:

- `BALSAM_TEST_DIR`: ephemeral test site will be created in this directory; should be somewhere readable from both the test machine and the compute/launch nodes.
- `BALSAM_LOG_DIR`: artifacts of the run saved here
- `BALSAM_TEST_API_URL`: http:// URL of the server to test against
- `BALSAM_TEST_PLATFORM`: the PLATFORM value such as "alcf_theta"

`BALSAM_TEST_DB_URL` is ignored if you are testing against an existing API server with `BALSAM_TEST_API_URL`.

Also important in the `test_platform.py` is the dictionary `LAUNCHER_STARTUP_TIMEOUT_SECONDS` describing how long the test runner should wait for a launcher to start. 
This is highly platform dependent and should be set based on the expected
queueing time in the test queue.

## Test Site configuration

Next in `balsam/tests/default-configs/` there is a default Site config directory for each value of  `PLATFORMS`.
You will need to add a config directory with a name matching the platform string added to `PLATFORMS`. 

This is basically just a clone of the user-facing default Site config located under:  `balsam/config/defaults/`.  However, the Site should be configured with any Apps or resources needed by the platform tests.
For instance, the integration tests *specifically* require the `hello.Hello` to exist in every Site.  This App can be copied from the `balsam/tests/default-configs/generic/apps/hello.py`. You may define additional platform specific Apps here. Moreover, the tests run under the first `project` and `queue` as defined in `settings.yml`.

## Running Tests

You should have installed the development dependencies into a virtualenv with
`make install-dev`.  

To run the integration tests from a login node: `cd` into the balsam root directory, set the environment variables above, and run `make test-site-integ`.

## Writing platform-specific tests

The actual tests are detected from files named `tests/site_integration/test_*.py`. A generic test `test_multi_job` runs 3 `hello.Hello` jobs and waits for them to reach JOB_FINISHED, repeating the test with both `serial` and `mpi` job modes.

By default, new test cases will run on *every platform*. 

To define a *platform-specific* test, simply use the `pytest.mark` decorator.
This indicates that a test case should only run when the  `BALSAM_TEST_PLATFORM` environment variable matches the platform marker name:

```py3
@pytest.mark.alcf_theta
def test_ATLAS_workflow() -> None:
    assert 1
```

### Using the test fixtures

The PyTest fixtures (defined in `conftest.py` files and the various test files) are responsible for all the test setup and teardown. Some useful fixtures include:

- `balsam_site_config`: creates an ephemeral Site and returns the SiteConfig object
- `client` returns a Balsam API client authenticated as the test Site owner
- `live_launcher` is a fixture parameterized in the job mode, meaning any test using it will be automatically repeated *twice* with both `serial` and `mpi` job modes running on a single node.  The fixture blocks until the launcher is actually up and running (this is where the `LAUNCHER_STARTUP_TIMEOUT_SECONDS` environment variable comes into play.)

For instance,  the following snippet applies the `live_launcher` fixture 
to all test cases in the `TestSingleNodeLaunchers` class:

```py3
@pytest.mark.usefixtures("live_launcher")
class TestSingleNodeLaunchers:
    @pytest.mark.parametrize("num_jobs", [3])
    def test_multi_job(self, balsam_site_config: SiteConfig, num_jobs: int, client: RESTClient) -> None:
        """
        3 hello world jobs run to completion
        """
        ...
```
