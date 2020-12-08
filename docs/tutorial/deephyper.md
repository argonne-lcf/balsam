# DeepHyper: Hyperparameter Search on Theta

Keras MNIST-MLP Benchmark
-------------------------

Let's search for optimal hyperparameters in the Keras 
[MNIST multilayer perceptron benchmark](https://github.com/keras-team/keras/blob/master/examples/mnist_mlp.py).
Notice the top-level comment: "there is *a lot* of margin for parameter
tuning," which underscores how much even a simple model can benefit from
hyperparameter optimization.

To start on Theta, let's set up a clean workspace and download the Keras
benchmark model and MNIST data.

```bash
# Create a new workspace with a Balsam DB
$ module unload balsam   # unload Balsam module: we want to use deephyper which comes with everything
$ module load deephyper/0.1.6  #  includes Balsam, Tensorflow, Keras, etc...
$ rm -r ~/.balsam # reset default settings (for now)
$ mkdir ~/dh-tutorial
$ cd ~/dh-tutorial
$ balsam init db
$ . balsamactivate ./db

# Grab the Keras MNIST-MLP Benchmark
# Run it on the login node  just long enough that the dataset can be downloaded
$ wget https://raw.githubusercontent.com/keras-team/keras/master/examples/mnist_mlp.py

$ python -c 'from keras.datasets import mnist; mnist.load_data()' # download dataset on login node
$ ls ~/.keras/datasets/mnist.npz
```

Defining the Search Space
-------------------------

Now let's have a look at the MNIST-MLP code. We immediately notice some
arbitrary choices for hyperparameters that we'd like to vary,
highlighted in the lines below:

```python
batch_size = 128 # **
num_classes = 10
epochs = 20

# the data, split between train and test sets
(x_train, y_train), (x_test, y_test) = mnist.load_data()

x_train = x_train.reshape(60000, 784)
x_test = x_test.reshape(10000, 784)
x_train = x_train.astype('float32')
x_test = x_test.astype('float32')
x_train /= 255
x_test /= 255
print(x_train.shape[0], 'train samples')
print(x_test.shape[0], 'test samples')

# convert class vectors to binary class matrices
y_train = keras.utils.to_categorical(y_train, num_classes)
y_test = keras.utils.to_categorical(y_test, num_classes)

model = Sequential()
model.add(Dense(512, activation='relu', input_shape=(784,))) # **
model.add(Dropout(0.2)) # **
model.add(Dense(512, activation='relu')) # **
model.add(Dropout(0.2)) # **
model.add(Dense(num_classes, activation='softmax'))

model.summary()

model.compile(loss='categorical_crossentropy',
              optimizer=RMSprop(), # **
              metrics=['accuracy'])
```

We start to wonder if there are better combinations of these six
hyperparameters:

 -   batch\_size
 -   number of units in the first hidden layer
 -   number of units in the second hidden layer
 -   dropout ratio in the first hidden layer
 -   dropout ratio in the second hidden layer
 -   choice of optimization algorithm

Suppose we are on a tight budget and are willing to sacrifice a little
accuracy for significantly fewer hidden units. Let's do a search over
these hyperparameters, with a highly restricted range on the number of
hidden units in both layers. In order to define the DeepHyper search
space over these parameters, we create a `problem.py` file that
defines the search problem. Let's set this up by creating the following
file:

``` {.python}
# problem.py
from deephyper.benchmark import HpProblem
Problem = HpProblem()

Problem.add_dim('log2_batch_size', (5, 10))
Problem.add_dim('nunits_1', (10, 100))
Problem.add_dim('nunits_2', (10, 30))
Problem.add_dim('dropout_1', (0.0, 1.0))
Problem.add_dim('dropout_2', (0.0, 1.0))
Problem.add_dim('optimizer_type', ['RMSprop', 'Adam'])
Problem.add_starting_point(
    log2_batch_size=7, nunits_1=100, nunits_2=20,
    dropout_1=0.2, dropout_2=0.2, optimizer_type='RMSprop'
)
```

Notice that the call to `Problem.add_dim()` takes two arguments:

   -   the hyperparameter name
   -   the hyperparameter **range**

DeepHyper automatically recognizes the hyperparmeter **type** based on
the range. There are three possibile hyperparameter types:

   -   **Discrete:** pair of integers (as in `log2_batch_size`)
   -   **Continuous:** pair of floating-point numbers (as in
        `dropout_1`)
   -   **Categorical:** list of any JSON-serializable data, like
        strings (as in `optimizer_type`)

The call to `Problem.add_starting_point()` allows us to pass reference configurations that
will run before any new hyperparameters are sampled in the search.

Now all we have to do is adjust our model code to accept various points
in this space, rather than using the fixed set of hyperparmeters in the
model code on Github.

Interfacing to the Model
------------------------

Getting DeepHyper to call the model code requires a straightforward
modification of the script. We place the entire model
build/train/validate code inside a function called `run()`, which
accepts one argument: a dictionary of hyperparmeters. The dictionary
keys will match those defined in the `HpProblem`, and the values can
span the entire problem space.

Of course, the model code must actually unpack the dictionary items and
use them in configuration of the model build/train process. This is
illustrated in the code snippet below.

After the model validation step, the `run()` function must return
the optimization objective back to DeepHyper. Since the problem is cast
as a maximization, we will return the **validation accuracy** as our
model quality metric. 

The full, modified model source code should look like the following
after you have implemented the `run()` function (with proper
signature and return value) and tweaked the model to read in a
hyperparameter dictionary.

```python
from __future__ import print_function

import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout

def run(param_dict):
    batch_size = 2**param_dict['log2_batch_size']
    nunits_1 = param_dict['nunits_1']
    nunits_2 = param_dict['nunits_2']
    dropout_1 = param_dict['dropout_1']
    dropout_2 = param_dict['dropout_2']
    optimizer_type = param_dict['optimizer_type']

    num_classes = 10
    epochs = 20

    # the data, split between train and test sets
    (x_train, y_train), (x_test, y_test) = mnist.load_data()

    x_train = x_train.reshape(60000, 784)
    x_test = x_test.reshape(10000, 784)
    x_train = x_train.astype('float32')
    x_test = x_test.astype('float32')
    x_train /= 255
    x_test /= 255
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')

    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    model = Sequential()
    model.add(Dense(nunits_1, activation='relu', input_shape=(784,)))
    model.add(Dropout(dropout_1))
    model.add(Dense(nunits_2, activation='relu'))
    model.add(Dropout(dropout_2))
    model.add(Dense(num_classes, activation='softmax'))

    model.summary()

    model.compile(loss='categorical_crossentropy',
                optimizer=optimizer_type,
                metrics=['accuracy'])

    history = model.fit(x_train, y_train,
                        batch_size=batch_size,
                        epochs=epochs,
                        verbose=1,
                        validation_data=(x_test, y_test))
    score = model.evaluate(x_test, y_test, verbose=0)
    print('Test loss:', score[0])
    print('Test accuracy:', score[1])
    return score[1]
```

Launch an Experiment
--------------------

The deephyper Theta module has a convenience script included for quick
generation of DeepHyper Async Bayesian Model Search (AMBS) search jobs.
Simply pass the paths to the `mnist_mlp_run.py` script (containing
the **run()** function) and the **problem.py** file as follows:

```bash
$ deephyper balsam-submit -p problem.py -r mnist_mlp_run.py -t 20 -q debug-cache-quad -n 2 -A datascience -j serial hps test-mnist
```

The positional arguments `hps` and `test-mnist` denote a Hyperparameter search
job with the `test-mnist` workflow label. The necessary Balsam application to
run DeepHyper Asynchronous Model-Based Search (AMBS) is automatically created
for the job.  You can see the details of the created Balsam job with `balsam
ls`.  Finally, the launcher script is automatically submitted to Cobalt for the requested
nodes and walltime in serial job mode (`-j serial`).

When the job starts running, the DeepHyper execution backend will use the
Balsam API to identify how many compute nodes are available and spawn
model evaluation tasks dynamically.

Monitor Execution and Check Results
-----------------------------------

You can use Balsam to watch when the experiment starts running and track
how many models are running in realtime. Once the ambs task is RUNNING,
the `bcd` command line tool provides a convenient way to jump to
the working directory, which will contain the DeepHyper log and search
results in CSV or JSON format. Notice the objective value in the
second-to-last column of the `results.csv` file.

```bash
$ balsam ls
                              job_id |        name |        workflow | application |   state
--------------------------------------------------------------------------------------------
806aa9a8-5028-4409-97c8-4971feb6aa87 | run05-01-19 | mnist_mlp_dh.py | ambs        | RUNNING

$ . bcd 806
$ balsam ls
                              job_id |        name |        workflow |      application |        state
------------------------------------------------------------------------------------------------------
33ae4062-5a48-4602-8f98-fb645dd0b10a | task0       | mnist_mlp_dh.py | mnist_mlp_dh.run | JOB_FINISHED
806aa9a8-5028-4409-97c8-4971feb6aa87 | run05-01-19 | mnist_mlp_dh.py | ambs             | RUNNING
2026a35a-a686-4d34-b6b1-f870514fe0a3 | task1       | mnist_mlp_dh.py | mnist_mlp_dh.run | RUNNING

$ ls
deephyper.log  results.csv  results.json  run05-01-19.out
```
