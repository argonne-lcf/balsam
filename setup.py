#!/usr/bin/env python
# -*- coding: utf-8 -*-
import setuptools
import re
from setuptools.command import easy_install

"""
Monkey patch setuptools to write faster console_scripts with this format:
    import sys
    from mymodule import entry_function
    sys.exit(entry_function())
This is better.
(c) 2016, Aaron Christianson
http://github.com/ninjaaron/fast-entry_points
"""

TEMPLATE = r"""
# -*- coding: utf-8 -*-
# EASY-INSTALL-ENTRY-SCRIPT: '{3}','{4}','{5}'
__requires__ = '{3}'
import re
import sys
from {0} import {1}
if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit({2}())
""".lstrip()


@classmethod
def get_args(cls, dist, header=None):  # noqa: D205,D400
    """
    Yield write_script() argument tuples for a distribution's
    console_scripts and gui_scripts entry points.
    """
    if header is None:
        # pylint: disable=E1101
        header = cls.get_header()
    spec = str(dist.as_requirement())
    for type_ in "console", "gui":
        group = type_ + "_scripts"
        for name, ep in dist.get_entry_map(group).items():
            # ensure_safe_name
            if re.search(r"[\\/]", name):
                raise ValueError("Path separators not allowed in script names")
            script_text = TEMPLATE.format(ep.module_name, ep.attrs[0], ".".join(ep.attrs), spec, group, name)
            # pylint: disable=E1101
            args = cls._get_script_args(type_, name, header, script_text)
            for res in args:
                yield res


# pylint: disable=E1101
easy_install.ScriptWriter.get_args = get_args

setuptools.setup()
