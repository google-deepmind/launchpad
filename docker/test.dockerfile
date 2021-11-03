ARG base_image="python:3.8"
FROM $base_image

ARG python_version="3.8"
ARG abi="38"
ARG APT_COMMAND="apt-get -o Acquire::Retries=3 -y"

# Needed to disable interactive configuration by tzdata.
RUN ln -fs /usr/share/zoneinfo/Europe/Dublin /etc/localtime

RUN ${APT_COMMAND} update && ${APT_COMMAND} install --no-install-recommends \
  byobu \
  tmux \
  xterm

RUN python${python_version} -mpip install --upgrade setuptools wheel attrs==20.3.0 pytype

COPY dist /tmp/launchpad

RUN WHL=`ls /tmp/launchpad/*${abi}*.whl` && python${python_version} -mpip install $WHL[tensorflow,reverb,xmanager]

WORKDIR /
