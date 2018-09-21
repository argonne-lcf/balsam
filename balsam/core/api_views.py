from django.shortcuts import render, redirect
from balsam.core.models import BalsamJob
from django.http import JsonResponse, HttpResponseBadRequest

def list_tasks(request):
    if not request.is_ajax():
        return HttpResponseBadRequest()
    
    #print("Parsed GET dict:")
    #print('\n'.join(f"{p} == {request.GET[p]}" for p in request.GET))

    _jobs = BalsamJob.objects.values_list('job_id', 'name', 'workflow',
                                          'application', 'state',
                                          'queued_launch__scheduler_id',
                                          'num_nodes', 'ranks_per_node')
    return JsonResponse({"data": list(_jobs)})

def task_detail(request, id):
    task = BalsamJob.objects.get(pk=id)
    text = repr(task)
    result = '<table cellpadding="5" cellspacing="0" border="0" style="padding-left:50px;">\n'
    lines = text.split('\n')[2:-1]
    for line in lines:
        try: field, val = line.split(':', 2)
        except: continue
        else: result += f'<tr> <td> {field} </td> <td> {val} </td> </tr>\n'
    result += '</table>'
    return JsonResponse({"html": result})
