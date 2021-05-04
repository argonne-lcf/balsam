ALCF Computational Performance Workshop 2021
============================================

The following tutorial sequence has accompanying code examples available on
[Github](https://github.com/argonne-lcf/CompPerfWorkshop-2021/tree/main/00_workflows/balsam_demo).

We recommend following the tutorial at your own pace and running examples one by one.  However, you can also
download and run the self-contained example scripts in the repository.

To get started on Theta:

```bash
git clone https://github.com/argonne-lcf/CompPerfWorkshop-2021.git
cd CompPerfWorkshop-2021/00_workflows/balsam_demo
module load balsam
balsam init testdb
source balsamactivate testdb
ls # testdb created in current directory
```

Let's proceed to [try out the command line interface](cli.md).
