# ApplicationDefinition Design

## Security
All aspects of the App's execution are contained in a Site-local file and NEVER
stored or accessed by the public API:

- Environment variables
- Preamble (module loads, etc..)
- Preprocess fxn
- Postprocess fxn
- Command template & parameters
- Stage in files
- Stage out files
- Error & timeout handling options

The command is generated from a Jinja template that only takes pre-specified
parameters.  Each parameter is escaped with shlex.quote to prevent shell injection.

All job processing must take place in job workdir.  The job workdirs are
strictly under the data/ subdirectory. Any stage-in destination is expressed as
an relative path to the workdir. This prevents staging-in or out malicious code into
apps/
