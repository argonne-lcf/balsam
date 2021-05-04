# Confirm that you have a test DB activated:
balsam which

# Add 16 tasks with the script:
python app.py 16

# View the tasks:
BALSAM_LS_FIELDS=data balsam ls

# Now submit a job to run those tasks
# Important: Please modify the project (-A) and (-q) as necessary for your allocation/machine:
balsam submit-launch -n 2 -q training -A SDL_Workshop -t 5 --job-mode serial

# Use `watch balsam ls` to track the status of each task in your DB
BALSAM_LS_FIELDS=data  watch balsam ls
