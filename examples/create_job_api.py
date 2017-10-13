#!/usr/bin/python
import sys
import django
django.setup()
from django.conf import settings
from balsam import models

num=1
if len(sys.argv)>1:
    num=int(sys.argv[1])

for i in range(num):
    job = models.BalsamJob()
    job.job_id                 = models.BalsamJob.generate_job_id()
    job.working_directory      = models.BalsamJob.create_working_path(job.job_id)
    #job.site                   = 'cooley'
    #job.job_name               = 'helloworld'
    #job.job_description        = 'helloworld'
    #job.origin_id              = options['origin_id']
    #job.queue                  = 'default'
    #job.project                = 'visualization'
    job.wall_time_minutes      = 30
    job.num_nodes              = 1
    #job.processes_per_node     = 1
    job.application            = 'helloworld'
    job.config_file            = '/home/turam/helloworld.sh'
    #job.input_url              = options['input_url']
    #job.output_url             = options['output_url']
    job.save()
    print 'Created job', job.id



