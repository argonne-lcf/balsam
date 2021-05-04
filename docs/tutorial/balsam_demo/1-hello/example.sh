# Confirm that you have a test DB activated:
balsam which

# Define an app:
balsam app --name hello --executable "echo hello, "

# Add a couple instances of that app:
balsam job --name hello-world --workflow demo-hello   --app hello --args 'world!'  --yes
balsam job --name hello-workshop   --workflow demo-hello   --app hello --args 'workshop!' --ranks-per-node 2 --yes

# View your tasks:
balsam ls
echo "You can set BALSAM_LS_FIELDS to add more columns to ls view..."
BALSAM_LS_FIELDS=ranks_per_node:args balsam ls

# Now submit a job to run those tasks
# Important: Please modify the project (-A) and (-q) as necessary for your allocation/machine:
balsam submit-launch -n 1 -q training -A Comp_Perf_Workshop -t 5 --job-mode mpi

# Use `qstat` to track your Cobalt job
# Use `balsam ls` to track the status of each task within a job
# Use `. bcd <first-few-chars-of-job-id>` to cd into a task directory (once task state says RUNNING)
