FROM ubuntu:16.04

ARG BUILDER_UID=9999

ENV PATH /home/builder/.local/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    python3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py pip==18.1 \
    && rm -rf get-pip.py

RUN python3 -m pip install \
    Cython==0.29 \
    bump2version==0.5.10

RUN useradd --create-home --no-log-init --shell /bin/bash --uid $BUILDER_UID builder
USER builder
WORKDIR /home/builder
