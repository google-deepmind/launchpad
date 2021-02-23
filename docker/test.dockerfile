FROM ubuntu:20.04

WORKDIR /tmp/launchpad

# Needed to disable interactive configuration by tzdata.
RUN ln -fs /usr/share/zoneinfo/Europe/Dublin /etc/localtime

RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  byobu \
  python3.8-dev \
  python3-pip \
  python3-venv \
  python3-setuptools \
  tmux \
  xterm

RUN pip3 install --upgrade setuptools wheel

COPY dist /tmp/launchpad

RUN pip3 install /tmp/launchpad/*

COPY run_python_tests.sh .

ENV PYTHONPATH=/tmp/launchpad

CMD ["bash", "run_python_tests.sh"]
