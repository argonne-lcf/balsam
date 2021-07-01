FROM python:3.9.6-buster

WORKDIR /balsam

COPY balsam/ balsam
COPY requirements/ requirements
COPY tests/ tests
COPY setup.cfg .
COPY setup.py .
COPY Makefile .
COPY pyproject.toml .
COPY entrypoint.sh .

RUN apt-get update && apt-get install -y \
    -q mpich libmpich-dev \
    && rm -rf /var/lib/apt/lists/*

RUN make install-dev
RUN mkdir /balsam/log

ENTRYPOINT ["/balsam/entrypoint.sh"]
