#!/bin/bash -lx
#COBALT -A {{ project }}
#COBALT -n {{ num_nodes }}
#COBALT -q {{ queue }}
#COBALT -t {{ wall_time_min }}
#COBALT --attrs pubnet=true:enable_ssh=1:{% if optional_params.get("mig_count") %}mig-mode=true{% endif %}

{% if optional_params.mig_count == "2" %}
    cgi=9
    mig_count=2
    echo "Creating 2x MIG 3g.20gb (ID 9)"
{% elif optional_params.mig_count == "3" %}
    cgi=14
    mig_count=3
    echo "Creating 3x MIG 2g.10gb (ID 14)"
{% elif optional_params.mig_count == "7" %}
    cgi=19
    mig_count=7
    echo "Creating 7x MIG 1g.5gb (ID 19)"
{% else %}
    mig_count=0
    echo "Not using MIG"
{% endif %}

if [ "$mig_count" -gt "0" ]
then
    # Create MIG Compute Instances
    for i in $(seq 1 $mig_count)
    do
        mpirun -hostfile $COBALT_NODEFILE \
        -n {{ num_nodes }} -npernode 1 \
        nvidia-smi_mig -cgi "$cgi" -C
    done

    # Record Instance IDs in local /var/tmp:
    for host in $(cat $COBALT_NODEFILE)
    do
        gpu_file="/var/tmp/balsam-$host-gpulist.txt"
        mpirun -hostfile $COBALT_NODEFILE \
        --host $host -n 1 nvidia-smi -L > $gpu_file
        echo "Recorded GPU list for $host in $gpu_file"
        cat $gpu_file
    done
fi


export PMI_NO_FORK=1
export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
