import os
import re
import shutil
import unittest
import time
from balsam.platform.mpirun import MPIRun, ThetaAprun, SlurmRun, SummitJsrun


class MpirunTestMixin(object):
    def assertInPath(self, exe):
        which_exe = shutil.which(exe)
        self.assertTrue(which_exe is not None, f"'{exe}' not in PATH")

    def test_start(self):
        self.assertInPath(self.mpirun.launch_command)

        self.mpirun.start(os.getcwd(), self.script_output)

        retval = self.mpirun.wait()
        self.assertIsInstance(retval, int)
        self.assertEqual(retval, 0)

        ranks, size, reduce = self.parse_output(self.script_output)

        self.assertEqual(ranks, self.ranks)
        self.assertEqual(size, self.ranks)

        self.assertEqual(reduce, self.ranks * self.reduce_val)

    def test_poll(self):
        self.assertInPath(self.mpirun.launch_command)

        self.mpirun.start(os.getcwd(), self.script_output)

        retval = self.mpirun.poll()
        counter = 0
        count_limit = 100
        while retval is None:
            time.sleep(0.2)
            retval = self.mpirun.poll()
            counter += 1
            if counter > count_limit:
                break

        self.assertLessEqual(counter, count_limit)

    def test_terminate(self):
        self.assertInPath(self.mpirun.launch_command)

        self.mpirun.start(os.getcwd(), self.script_output)
        start = time.time()
        self.mpirun.terminate()
        self.mpirun.wait()
        end = time.time()
        self.assertLessEqual(end - start, self.sleep_sec)

    def test_kill(self):
        self.assertInPath(self.mpirun.launch_command)

        self.mpirun.start(os.getcwd(), self.script_output)
        start = time.time()
        self.mpirun.kill()
        self.mpirun.wait()
        end = time.time()
        self.assertLessEqual(end - start, self.sleep_sec)

    def test_wait(self):
        self.assertInPath(self.mpirun.launch_command)

        start = time.time()
        self.mpirun.start(os.getcwd(), self.script_output)
        retval = self.mpirun.wait()
        end = time.time()
        self.assertGreaterEqual(end - start, self.sleep_sec)
        self.assertIsInstance(retval, int)
        self.assertEqual(retval, 0)


class MPIRunTest(MpirunTestMixin, unittest.TestCase):
    reduce_val = 5
    sleep_sec = 2
    test_script = """from mpi4py import MPI
import time
c = MPI.COMM_WORLD
print('rank',c.Get_rank(),'of',c.Get_size())
x = {0}
y = c.allreduce(x,MPI.SUM)
print('reduce ',y)
time.sleep({1})
""".format(
        reduce_val, sleep_sec
    )
    script_fn = "temp.py"
    script_output = "temp.txt"

    ranks = 4
    ranks_per_node = 2

    def setUp(self):
        with open(self.script_fn, "w") as file:
            file.write(self.test_script)
        app_args = ["python", self.script_fn]
        node_list = ["nodeA", "nodeB"]
        num_ranks = self.ranks
        ranks_per_node = self.ranks_per_node
        env = os.environ
        self.mpirun = MPIRun(app_args, node_list, num_ranks, ranks_per_node, env=env)
        self.mpirun.get_launch_args = lambda: ["-n", num_ranks]

    @staticmethod
    def parse_output(output_fn):

        ranks = []
        sizes = []
        with open(output_fn) as file:
            for line in file:
                if line.startswith("rank"):
                    rank, size = re.findall(r"\d+", line)
                    ranks.append(int(rank))
                    sizes.append(int(size))
                elif line.startswith("reduce"):
                    reduce = re.findall(r"\d+", line)

        return len(set(ranks)), tuple(set(sizes))[0], int(reduce[0])

    def tearDown(self):
        os.remove(self.script_fn)
        os.remove(self.script_output)


class ThetaAprunTest(MpirunTestMixin, unittest.TestCase):
    reduce_val = 5
    sleep_sec = 2
    test_script = """from mpi4py import MPI
import time
c = MPI.COMM_WORLD
print('rank',c.Get_rank(),'of',c.Get_size())
x = {0}
y = c.allreduce(x,MPI.SUM)
print('reduce ',y)
time.sleep({1})
""".format(
        reduce_val, sleep_sec
    )
    script_fn = "temp.py"
    script_output = "temp.txt"

    ranks = 4
    ranks_per_node = 2

    python_exe = "/soft/datascience/conda/miniconda3/latest/bin/python"

    def setUp(self):
        with open(self.script_fn, "w") as file:
            file.write(self.test_script)
        app_args = [self.python_exe, self.script_fn]
        node_list = ["nodeA", "nodeB"]
        num_ranks = self.ranks
        ranks_per_node = self.ranks_per_node
        env = os.environ
        self.mpirun = ThetaAprun(
            app_args, node_list, num_ranks, ranks_per_node, env=env
        )
        self.mpirun.get_launch_args = lambda: ["-n", num_ranks]

    @staticmethod
    def parse_output(output_fn):

        ranks = []
        sizes = []
        with open(output_fn) as file:
            for line in file:
                if line.startswith("rank"):
                    rank, size = re.findall(r"\d+", line)
                    ranks.append(int(rank))
                    sizes.append(int(size))
                elif line.startswith("reduce"):
                    reduce = re.findall(r"\d+", line)

        return len(set(ranks)), tuple(set(sizes))[0], int(reduce[0])

    def tearDown(self):
        os.remove(self.script_fn)
        os.remove(self.script_output)


class SlurmRunTest(MpirunTestMixin, unittest.TestCase):
    reduce_val = 5
    sleep_sec = 2
    test_script = """from mpi4py import MPI
import time
c = MPI.COMM_WORLD
print('rank',c.Get_rank(),'of',c.Get_size())
x = {0}
y = c.allreduce(x,MPI.SUM)
print('reduce ',y)
time.sleep({1})
""".format(
        reduce_val, sleep_sec
    )
    script_fn = "temp.py"
    script_output = "temp.txt"

    ranks = 4
    ranks_per_node = 2

    python_exe = "/usr/common/software/python/3.7-anaconda-2019.10/bin/python"

    def setUp(self):
        with open(self.script_fn, "w") as file:
            file.write(self.test_script)
        app_args = [self.python_exe, self.script_fn]
        node_list = ["nodeA", "nodeB"]
        num_ranks = self.ranks
        ranks_per_node = self.ranks_per_node
        env = os.environ
        self.mpirun = SlurmRun(app_args, node_list, num_ranks, ranks_per_node, env=env)
        self.mpirun.get_launch_args = lambda: ["-n", num_ranks]

    @staticmethod
    def parse_output(output_fn):

        ranks = []
        sizes = []
        with open(output_fn) as file:
            for line in file:
                if line.startswith("rank"):
                    rank, size = re.findall(r"\d+", line)
                    ranks.append(int(rank))
                    sizes.append(int(size))
                elif line.startswith("reduce"):
                    reduce = re.findall(r"\d+", line)

        return len(set(ranks)), tuple(set(sizes))[0], int(reduce[0])

    def tearDown(self):
        os.remove(self.script_fn)
        os.remove(self.script_output)


class SummitJsrunTest(MpirunTestMixin, unittest.TestCase):
    reduce_val = 5
    sleep_sec = 2
    test_script = """from mpi4py import MPI
import time
c = MPI.COMM_WORLD
print('rank',c.Get_rank(),'of',c.Get_size())
x = {0}
y = c.allreduce(x,MPI.SUM)
print('reduce ',y)
time.sleep({1})
""".format(
        reduce_val, sleep_sec
    )
    script_fn = "temp.py"
    script_output = "temp.txt"

    ranks = 6
    ranks_per_node = 6
    gpus_per_rank = 1

    python_exe = "/sw/summit/python/3.6/anaconda3/5.3.0/bin/python"

    def setUp(self):
        with open(self.script_fn, "w") as file:
            file.write(self.test_script)
        app_args = [self.python_exe, self.script_fn]
        node_list = ["nodeA", "nodeB"]
        num_ranks = self.ranks
        ranks_per_node = self.ranks_per_node
        env = os.environ
        self.mpirun = SummitJsrun(
            app_args,
            node_list,
            num_ranks,
            ranks_per_node,
            env=env,
            gpus_per_rank=self.gpus_per_rank,
        )
        self.mpirun.get_launch_args = lambda: ["-n", num_ranks]

    @staticmethod
    def parse_output(output_fn):

        ranks = []
        sizes = []
        with open(output_fn) as file:
            for line in file:
                if line.startswith("rank"):
                    rank, size = re.findall(r"\d+", line)
                    ranks.append(int(rank))
                    sizes.append(int(size))
                elif line.startswith("reduce"):
                    reduce = re.findall(r"\d+", line)

        return len(set(ranks)), tuple(set(sizes))[0], int(reduce[0])

    def tearDown(self):
        os.remove(self.script_fn)
        os.remove(self.script_output)


if __name__ == "__main__":
    import logging

    logging.basicConfig()
    unittest.main()
