# First, copy this directory to a writeable location
# To follow along, `source` this script or type the commands in your shell
# Let's start by making a DB to experiment with:

# On Theta:
module load balsam

# Use this instead on Cooley:
# source /soft/datascience/balsam/setup.sh


# Create a DB named 'testdb' to experiment with:
balsam init testdb
source balsamactivate testdb

# Notice the db is now in the current folder:
ls
