import os,logging,sys
from django.shortcuts import render,get_object_or_404
from balsam.argo.html_forms import JobDisplayForm
logger = logging.getLogger(__name__)

from balsam.argo import models

# Create your views here.

def index(request):
   argo_jobs = models.ArgoJob.objects.all()
   context = {'argo_jobs': argo_jobs}
   return render(request, 'argo/index.html', context)


def filter(request):
   argo_jobs = models.ArgoJob.objects.all()
   context = {'argo_jobs': argo_jobs }
   return render(request, 'argo/filter.html', context)


def job_display(request,job_num):
   logger.debug('inside job_display')
   argo_job = None
   argo_subjobs = None
   form = None
   if job_num is not None:
      argo_job = models.ArgoJob.objects.get(id=int(job_num))
      argo_subjob_pks = argo_job.get_subjob_pk_list()
      argo_subjobs = models.ArgoSubJob.objects.filter(pk__in=argo_subjob_pks)
      current_subjob_choices = []
      for i in range(len(argo_subjob_pks)):
         current_subjob_choices.append( (i,str(i)))
      if argo_job is not None:
         form = None
         if request.method == 'POST':
            form = JobDisplayForm(current_subjob_choices,request.POST)
            if form.is_valid():
               if 'delete_from_database' in request.POST:
                  message = 'Job Number ' + str(job_num)
                  if request.POST.get('confirm_delete'):
                     argo_job.delete()
                     message += ' Deleted'
                  else:
                     message += ' not deleted, go back and click the check box to confirm.'
                  context = {'message':message}
                  return render(request,'argo/job_deleted.html',context)
               elif 'save_to_database' in request.POST:
                  update_fields = []
                  new_state    = form.cleaned_data['state']
                  if argo_job.state != new_state:
                     argo_job.state = new_state
                     update_fields.append('state')
                  new_group_identifier = form.cleaned_data['group_identifier']
                  if argo_job.group_identifier != new_group_identifier:
                     argo_job.group_identifier = new_group_identifier
                     update_fields.append('group_identifier')
                  new_email = form.cleaned_data['email']
                  if argo_job.email != new_email:
                     argo_job.email = new_email
                     update_fields.append('email')
                  argo_job.save(update_fields=update_fields)
         else: # not a POST, but a GET
            form = JobDisplayForm(current_subjob_choices)
            form.fields['state'].initial              = argo_job.state
            form.fields['group_identifier'].initial   = argo_job.group_identifier
            form.fields['email'].initial              = argo_job.email
            form.fields['current_subjob'].initial     = current_subjob_choices
   context = {'argo_job':argo_job,'job_num': job_num, 'form':form, 'argo_subjobs': argo_subjobs}
   return render(request,'argo/job_display.html',context)

