'''
The Balsam Launcher is a pilot application that runs either directly on compute
nodes or very close to them (perhaps on a service node with ``mpirun`` access). 
The Launcher is responsible for:

    * **pulling jobs** from the Balsam database

    * **stage-in and stage-out** transitions that include working
      directory creation, remote and local file transfer, and the enabling the
      requested flow of files from parent to child jobs transparently

    * running custom **pre- and post-processing** scripts for each job

    * invoking **job execution** on the appropriate resources

    * **monitoring** job execution and providing resilient mechanisms to **handle
      expected and unexpected** runtime errors or job timeouts

In normal Balsam usage, the Launcher is not invoked directly. Instead, multiple
Launcher instances with specific workloads are automatically scheduled for execution by the
Metascheduler.  However, the Launcher can also simply be invoked from the
command line.  For example, to consume all jobs from the database, use:

>>> $ balsam launcher --consume-all 
'''

import logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
