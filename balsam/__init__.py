import logging
import textwrap
import sys

__version__ = "0.0.1"

root_logger = logging.getLogger("balsam")
root_logger.setLevel(logging.DEBUG)
stderr_handler = logging.StreamHandler()
root_logger.addHandler(stderr_handler)


def banner(message, color="HEADER"):
    bcolors = {
        "HEADER": "\033[95m",
        "OKBLUE": "\033[94m",
        "OKGREEN": "\033[92m",
        "WARNING": "\033[93m",
        "FAIL": "\033[91m",
        "ENDC": "\033[0m",
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m",
    }
    message = "\n".join(l.strip() for l in message.split("\n"))
    lines = textwrap.wrap(message, width=80)
    width = max(len(l) for l in lines) + 4
    header = "*" * width
    msg = f" {header}\n"
    for l in lines:
        msg += "   " + l + "\n"
    msg += f" {header}"
    if sys.stdout.isatty():
        print(bcolors.get(color), msg, bcolors["ENDC"], sep="")
    else:
        print(msg)


__all__ = ["banner"]
