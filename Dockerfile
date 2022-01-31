FROM ubuntu:latest

ARG BUILDER_UID=9999
ARG DEBIAN_FRONTEND=noninteractive

ENV PATH /home/builder/.local/bin:$PATH
ENV PYTHON_VERSION 3.8.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    python3-dev \
    wget \
    # Pyenv pre-requisites
    make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# Set-up necessary Env vars for PyEnv
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

# Install pyenv
RUN set -ex \
    && curl https://pyenv.run | bash \
    && pyenv install $PYTHON_VERSION \
    && pyenv global $PYTHON_VERSION \
    && pyenv rehash \
    && chmod -R a+w $PYENV_ROOT/shims

RUN pip install \
    Cython==0.29 \
    bump2version==0.5.10 \
    wheel

RUN useradd --create-home --no-log-init --shell /bin/bash --uid $BUILDER_UID builder
USER builder
WORKDIR /home/builder

# Optional : Checks Pyenv version on container start-up
# ENTRYPOINT [ "pyenv","version" ]
