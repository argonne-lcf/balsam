import logging
import logging.handlers
import os
from typing import Optional, TextIO

from gunicorn import util  # type: ignore
from gunicorn.glogging import Logger  # type: ignore


class RotatingGunicornLogger(Logger):  # type: ignore
    def _set_handler(
        self, log: logging.Logger, output: str, fmt: logging.Formatter, stream: Optional[TextIO] = None
    ) -> None:
        # remove previous gunicorn log handler
        h = self._get_gunicorn_handler(log)
        if h:
            log.handlers.remove(h)

        if output is not None:
            if output == "-":
                h = logging.StreamHandler(stream)
            else:
                util.check_is_writeable(output)
                h = logging.handlers.RotatingFileHandler(
                    output,
                    maxBytes=int(32 * 1e6),
                    backupCount=3,
                )
                # make sure the user can reopen the file
                try:
                    os.chown(h.baseFilename, self.cfg.user, self.cfg.group)
                except OSError:
                    # it's probably OK there, we assume the user has given
                    # /dev/null as a parameter.
                    pass

            h.setFormatter(fmt)
            h._gunicorn = True
            log.addHandler(h)
