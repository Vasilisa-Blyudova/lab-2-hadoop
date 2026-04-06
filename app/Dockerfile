FROM bde2020/spark-master:3.1.1-hadoop3.2 AS python-builder

ENV PYTHON_VERSION=3.11.11

ENV PATH="/usr/local/bin:${PATH}"

RUN apk add --no-cache \
    build-base \
    bzip2-dev \
    libffi-dev \
    linux-headers \
    ncurses-dev \
    openssl-dev \
    readline-dev \
    sqlite-dev \
    tar \
    tk-dev \
    wget \
    xz-dev \
    zlib-dev

RUN wget -O /tmp/python.tgz "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
RUN tar -xzf /tmp/python.tgz -C /tmp
WORKDIR /tmp/Python-${PYTHON_VERSION}
RUN ./configure --prefix=/usr/local --with-ensurepip=install
RUN make -j"$(getconf _NPROCESSORS_ONLN)"
RUN make altinstall
RUN ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3
RUN ln -sf /usr/local/bin/pip3.11 /usr/local/bin/pip3
RUN rm -rf /tmp/python.tgz "/tmp/Python-${PYTHON_VERSION}"

WORKDIR /opt/spark-app

COPY requirements.txt .

RUN python3 -m pip install --no-cache-dir -r requirements.txt


FROM bde2020/spark-master:3.1.1-hadoop3.2

ENV PATH="/usr/local/bin:${PATH}"

RUN apk add --no-cache \
    bzip2 \
    libffi \
    libstdc++ \
    ncurses-libs \
    openssl \
    readline \
    sqlite-libs \
    tk \
    xz-libs \
    zlib
RUN mkdir -p /usr/local

COPY --from=python-builder /usr/local /usr/local

RUN ln -sf /usr/local/bin/python3 /usr/bin/python3 && \
    ln -sf /usr/local/bin/pip3 /usr/bin/pip3

WORKDIR /opt/spark-app

COPY . .
