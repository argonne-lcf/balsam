FROM python:3.9.1-buster

WORKDIR /balsam

COPY balsam/ balsam
COPY requirements/ requirements
COPY tests/ tests
COPY setup.cfg .
COPY setup.py .
COPY Makefile .
COPY pyproject.toml .
COPY fastentrypoints.py .
COPY entrypoint.sh .

RUN make install-dev
RUN mkdir /balsam/log

ENTRYPOINT ["/balsam/entrypoint.sh"]
