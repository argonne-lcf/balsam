.. role:: bash(code)
   :language: bash

DeepHyper: Hyperparameter Search on Theta
==========================================

Keras MNIST-MLP Benchmark
---------------------------
Let's search for optimal hyperparameters in the Keras `MNIST 
multilayer perceptron benchmark <https://github.com/keras-team/keras/blob/master/examples/mnist_mlp.py>`_. 
Notice the top-level comment: "there is *a lot* of margin for parameter tuning," which underscores
how much even a simple model can benefit from hyperparameter optimization.

To start, let's set up a clean workspace and download the Keras benchmark model and MNIST data.

.. code-block:: bash

    # Create a new workspace with a Balsam DB
    $ module load deephyper
    $ mkdir ~/dh-tutorial
    $ cd ~/dh-tutorial
    $ balsam init db
    $ . balsamactivate ./db

    # Grab the Keras MNIST-MLP Benchmark
    # Run it on the login node  just long enough that the dataset can be downloaded
    $ wget https://raw.githubusercontent.com/keras-team/keras/master/examples/mnist_mlp.py

    $ python mnist_mlp.py # CTRL+C after download finished and training starts
    $ ls ~/.keras/datasets/mnist.npz


Defining the Search Space
--------------------------
Now let's have a look at the MNIST-MLP code.  We immediately notice some arbitrary
choices for hyperparameters that we'd like to vary, highlighted in the lines below:

.. code-block:: python
    :emphasize-lines: 1,22,23,24,25,31

    batch_size = 128
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
    model.add(Dense(512, activation='relu', input_shape=(784,)))
    model.add(Dropout(0.2))
    model.add(Dense(512, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(num_classes, activation='softmax'))

    model.summary()

    model.compile(loss='categorical_crossentropy',
                  optimizer=RMSprop(),
                  metrics=['accuracy'])

We start to wonder if there are better combinations of these six hyperparameters:

  - batch_size
  - number of units in the first hidden layer
  - number of units in the second hidden layer
  - dropout ratio in the first hidden layer
  - dropout ratio in the second hidden layer
  - choice of optimization algorithm
 
Suppose we are on a tight budget and are willing to sacrifice a little accuracy
for significantly fewer hidden units.  Let's do a search over these hyperparameters,
with a highly restricted range on the number of hidden units in both layers.
In order to define the DeepHyper search space over these parameters, we create a :bash:`problem.py` 
file that defines the search problem. Let's set this up by creating the following file:

.. code-block:: python
    :caption: **problem.py**

    from deephyper.benchmark import HpProblem
    Problem = HpProblem()

    Problem.add_dim('log2_batch_size', (5, 10), 7)
    Problem.add_dim('nunits_1', (10, 100), 100)
    Problem.add_dim('nunits_2', (10, 30), 20)
    Problem.add_dim('dropout_1', (0.0, 1.0), 0.2)
    Problem.add_dim('dropout_2', (0.0, 1.0), 0.2)
    Problem.add_dim('optimizer_type', ['RMSprop', 'Adam'], 'RMSprop')


Notice that the call to Problem.add_dim() takes three arguments:
  - the hyperparameter name
  - the hyperparameter **range**
  - the hyperparameter reference value (optional)

DeepHyper automatically recognizes the hyperparmeter **type** based on the range.

There are three possibilities:
  - **Discrete:** pair of integers (as in *log2_batch_size*)
  - **Continuous:** pair of floating-point numbers (as in *dropout_1*)
  - **Categorical:** list of any JSON-serializable data, like strings (as in *optimizer_type*)


Now all we have to do is adjust our model code to accept various points in this space,
rather than using the fixed set of hyperparmeters in the model code on Github.

Interfacing to the Model
-------------------------
Wrapping in a run() function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Getting DeepHyper to call the model code requires a straightforward modification of the script.
We place the entire model build/train/validate code inside a function called :bash:`run()`, which
accepts one argument: a dictionary of hyperparmeters.  The dictionary keys will match those defined in the 
**HpProblem**, and the values can span the entire problem space.

Using the hyperparameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Of course, the model code must actually unpack the dictionary items and use them in
configuration of the model build/train process. This is illustrated in the code snippet below.

Returning a minimization objective
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After the model validation step, the :bash:`run()` function must return the optimization objective back to
DeepHyper. Since the problem is cast as a **minimization**, we will return the **negative validation accuracy**,
which will be equivalent to maximizing accuracy under DeepHyper. 

The full modified model code should look like this:

.. code-block:: python
    :caption: **mnist_mlp_dh.py**
    
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
        return -score[1]

Launch an Experiment
----------------------
The deephyper Theta module has a convenience script included for quick generation  
of DeepHyper Async Bayesian Model Search (AMBS) search jobs. Simply pass the 
paths to the **mnist_mlp_dh.py** script (containing the **run()** function) and the **problem.py** 
file as follows:

.. code-block:: bash

    $ deephyper-ambs  mnist_mlp_dh.py problem.py

You will see the details of the created Balsam job to run the DeepHyper AMBS search code.
Internally, the DeepHyper execution backend uses the Balsam API  to identify how many 
compute nodes are available and spawn model evaluation tasks dynamically. To start the
search, simply dispatch a launcher job with as many nodes as you like:

.. code-block:: bash

    $ balsam submit-launch -n 8 -t 60 -A Project -q Queue --job-mode=serial

Monitor Execution and Check Results
---------------------------------------
You can use Balsam to watch when the experiment starts running and track how many models are running in realtime.
The :bash:`bcd` command line tool provides a convenient way to jump to the working directory of the AMBS task, which 
will contain the search results in CSV and JSON formats. Notice the objective
value in the second-to-last column of the :bash:`results.csv` file.

.. highlight:: console

::

    $ balsam ls
    . bcd <first-few-char-of-job-id>