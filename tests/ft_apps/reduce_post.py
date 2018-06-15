#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsam.launcher.dag as dag

print("Hello from reduce_post")

for i in range(5):
    with open(f"summary{i}.dat", 'w') as fp:
        fp.write("test\n")
