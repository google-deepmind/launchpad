ARG base_image="python:3.8"
FROM $base_image

ARG python_version="3.8"
ARG abi="38"

# Needed to disable interactive configuration by tzdata.
RUN ln -fs /usr/share/zoneinfo/Europe/Dublin /etc/localtime

RUN apt-get update && apt-get install -y --no-install-recommends \
  byobu \
  tmux \
  xterm

RUN python${python_version} -mpip install --upgrade setuptools wheel attrs==20.3.0 pytype

COPY dist /tmp/launchpad

RUN WHL=`ls /tmp/launchpad/*${abi}*.whl` && python${python_version} -mpip install $WHL[tensorflow,reverb]

WORKDIR /
