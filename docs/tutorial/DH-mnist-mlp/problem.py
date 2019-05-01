from deephyper.benchmark import HpProblem
Problem = HpProblem()

Problem.add_dim('log2_batch_size', (5, 10), 7)
Problem.add_dim('nunits_1', (10, 100), 100)
Problem.add_dim('nunits_2', (10, 30), 20)
Problem.add_dim('dropout_1', (0.0, 1.0), 0.2)
Problem.add_dim('dropout_2', (0.0, 1.0), 0.2)
Problem.add_dim('optimizer_type', ['RMSprop', 'Adam'], 'RMSprop')
