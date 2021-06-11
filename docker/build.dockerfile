ARG cpu_base_image="tensorflow/tensorflow:2.2.0-custom-op-ubuntu16"
ARG base_image=$cpu_base_image
FROM $base_image

LABEL maintainer="Launchpad Team <no-reply@google.com>"
ARG tensorflow_pip="tf-nightly"
ARG python_version="3.8"
ARG APT_COMMAND="apt-get -o Acquire::Retries=3 -y"

# Pick up some TF dependencies

RUN ${APT_COMMAND} update && ${APT_COMMAND} install -y --no-install-recommends \
        software-properties-common \
        aria2 \
        build-essential \
        curl \
        git \
        less \
        libfreetype6-dev \
        libhdf5-serial-dev \
        libpng-dev \
        libzmq3-dev \
        lsof \
        pkg-config \
        python3-dev \
        python3.6-dev \
        python3.7-dev \
        python3.8-dev \
        python3.9-dev \
        # python >= 3.8 needs distutils for packaging.
        python3.8-distutils \
        python3.9-distutils \
        rename \
        rsync \
        sox \
        unzip \
        vim \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -O https://bootstrap.pypa.io/get-pip.py

ARG pip_dependencies=' \
      absl-py \
      contextlib2 \
      dm-tree>=0.1.5 \
      google-api-python-client \
      h5py \
      numpy \
      mock \
      oauth2client \
      pandas \
      portpicker'

RUN for version in ${python_version}; do \
    python$version get-pip.py && \
    python$version -mpip uninstall -y tensorflow tensorflow-gpu tf-nightly tf-nightly-gpu && \
    python$version -mpip --no-cache-dir install ${tensorflow_pip} --upgrade && \
    python$version -mpip --no-cache-dir install $pip_dependencies; \
  done
RUN rm get-pip.py

# Needed until this is included in the base TF image.
RUN ln -s "/usr/include/x86_64-linux-gnu/python3.8" "/dt7/usr/include/x86_64-linux-gnu/python3.8"
RUN ln -s "/usr/include/x86_64-linux-gnu/python3.8" "/dt8/usr/include/x86_64-linux-gnu/ppython3.8"
RUN ln -s "/usr/include/x86_64-linux-gnu/python3.9" "/dt7/usr/include/x86_64-linux-gnu/python3.9"
RUN ln -s "/usr/include/x86_64-linux-gnu/python3.9" "/dt8/usr/include/x86_64-linux-gnu/ppython3.9"

CMD ["/bin/bash"]
