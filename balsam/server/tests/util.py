import atexit
import json
import logging
import os

from django.template import Template, Context
from django.conf import settings
from django.db import connection, reset_queries


class QueryLogger:
    def __init__(self):
        logger = logging.getLogger("django.db.backends")
        logger.addHandler(logging.StreamHandler())
        self.logger = logger

    def __enter__(self):
        if os.environ.get("LOG_SQL"):
            self.logger.setLevel(logging.DEBUG)
            settings.DEBUG = True

    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.setLevel(logging.INFO)
        settings.DEBUG = False


class QueryProfiler:
    is_enabled = os.environ.get("QUERY_REPORT", False)
    atexit_registered = False
    template = Template(
        """{{title}}: {{count}} quer{{count|pluralize:\"y,ies\"}} in {{time}} seconds:
        {% for sql in sqllog %}[{{forloop.counter}}] {{sql.time}}s: {{sql.sql|safe}}{% if not forloop.last %}
        {% endif %}{% endfor %}"""
    )
    reports = []

    def __init__(self, title):
        self.title = title
        if not QueryProfiler.atexit_registered:
            atexit.register(QueryProfiler.print_reports)
            QueryProfiler.atexit_registered = True

    def __enter__(self):
        if self.is_enabled:
            settings.DEBUG = True
            reset_queries()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_enabled:
            self.add_report()
            reset_queries()
            settings.DEBUG = False

    def add_report(self):
        elapsed = sum([float(q["time"]) for q in connection.queries])
        report = self.template.render(
            Context(
                {
                    "title": self.title,
                    "sqllog": connection.queries,
                    "count": len(connection.queries),
                    "time": elapsed,
                }
            )
        )
        self.reports.append(report)

    @classmethod
    def print_reports(cls):
        if cls.is_enabled:
            print("\n  SUMMARY OF SQL QUERIES\n" + "*" * 24)
            for report in cls.reports:
                print(report)


def pretty_data(data):
    return "\n" + json.dumps(data, indent=2)
