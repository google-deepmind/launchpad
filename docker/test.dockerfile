ARG base_image="ubuntu:20.04"
FROM $base_image

ARG python_version="3.8"
ARG abi="38"

WORKDIR /tmp/launchpad

# Needed to disable interactive configuration by tzdata.
RUN ln -fs /usr/share/zoneinfo/Europe/Dublin /etc/localtime

RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  byobu \
  python${python_version}-dev \
  python3-pip \
  tmux \
  xterm

RUN python${python_version} -mpip install --upgrade setuptools wheel

COPY dist /tmp/launchpad

RUN python${python_version} -mpip install /tmp/launchpad/*${abi}*.whl

COPY run_python_tests.sh .

ENV PYTHONPATH=/tmp/launchpad
