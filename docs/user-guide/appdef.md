# ApplicationDefinition Design

You can add Apps to modules in the Site `apps/` folder, with multiple apps per Python module file and multiple files.
Every Site comes "pre-packaged" with some default Apps that Balsam developers have pre-configured for that particular HPC system.

You are to modify, delete, and add your own apps.  To make a new app, you can simply copy one of the existing `App` modules as a 
starting point, or you can use the command line interface to generate a new template app:

```
$ balsam app create
Application Name (of the form MODULE.CLASS): test.Hello
Application Template [e.g. 'echo Hello {{ name }}!']: echo hello {{ name }} && sleep {{ sleeptime }} && echo goodbye
```

Now open `apps/test.py` and see the `Hello` class that was generated for you.  The allowed parameters for this App are given in 
double curly braces: `{{ name }}` and `{{ sleeptime }}`.  When you add `test.Hello` jobs, you will have to pass these two parameters 
and Balsam will take care of building the command line.  

The other components of an App are also defined directly on the `ApplicationDefinition` class, rather than in other files:

- `preprocess()` will run on Jobs immediately before `RUNNING`
- `postprocess()` will run on Jobs immediately after `RUN_DONE`
- `shell_preamble()` takes the place of the `envscript`: return a multiline string envscript or a `list` of commands
- `handle_timeout()` will run immediately after `RUN_TIMEOUT`
- `handle_error()` will run immediately after `RUN_ERROR`

Whenever you have changed your `apps/` directory, you need to inform the API about the changes.  All it takes is a single command
to sync up:
```
$ balsam app sync
$ balsam app ls
# Now you should see your newly-created App show up
```

Note that the API does not store *anything* about the `ApplicationDefinition` classes other than the class name and some metadata
about allowed parameters, allowed data transfers, etc...  What actually runs at the Site is determined entirely from the class
on the local filesystem.