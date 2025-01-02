To add Sophia to Balsam default configs, you have to add the following files:

- add sophia.py in platform/app_run and update the __init__.py in the folder
- add alcf_sophia_node.py in platform/compute_node and update the __init__.py in the folder
- add alcf_sophia folder with proper job-template.sh and settings.yml files to the config/defaults folder

The files are in my forked repo (fbhuiyan2-patch-1). Once these adjustments are made, Balsam will show Sophia as an option when opening a new site.

I have used the Sophia configuration to carry out VASP, LAMMPS, and Python jobs. Jobs are executed properly, node packing also works as expected. 
