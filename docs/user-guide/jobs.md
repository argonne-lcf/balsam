# Balsam Jobs

## The Balsam Job Lifecycle

## Accessing Parent Jobs

```python
def preprocess(self):
    parents = self.job.parent_query()
    for parent in parents:
        print("Parent workdir:", parent.workdir)
    self.job.state = "PREPROCESSED"
```