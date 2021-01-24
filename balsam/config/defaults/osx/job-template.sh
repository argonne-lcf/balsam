#!/bin/bash

echo "This demonstrates a Balsam job-template rendering with Jinja"
echo "Balsam site path is: {{balsam_site_path}}"

echo "Project: {{project}}"
echo "Queue: {{queue}}"
echo "num_nodes: {{num_nodes}}"
echo "wall_time_min: {{wall_time_min}}"
echo "job_mode: {{job_mode}}"

echo "filter_tags:" 
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %}

{% if optional_params.get("shout") == "yes" %}
echo "SHOUTING:  {{ optional_params['shout'] | upper }}"
{% else %}
echo "You did not provide the 'shout' optional parameter"
{% endif %}

export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
