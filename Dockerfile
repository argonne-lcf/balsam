FROM python:3-slim

WORKDIR /balsam

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y wget && \
    apt-get install -y lsb-release && \
    apt-get install -y gnupg && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get install -y build-essential && \
    apt-get install -y postgresql && \
    apt-get install -y libpq-dev && \
    apt-get clean all && \
    rm -rf /var/lib/apt/lists/*

COPY balsam/ balsam
COPY requirements/ requirements
COPY tests/ tests
COPY setup.cfg .
COPY setup.py .
COPY Makefile .
COPY pyproject.toml .
COPY entrypoint.sh .

RUN pip install --upgrade pip && pip install -r requirements/deploy.txt
RUN mkdir /balsam/log

ENTRYPOINT ["/balsam/entrypoint.sh"]
