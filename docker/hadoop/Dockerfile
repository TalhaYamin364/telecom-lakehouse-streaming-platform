# Java 11.0.x on Ubuntu 22.04, pinned by digest; it already ships curl, tar and CA certificates.
FROM eclipse-temurin:11-jre-jammy@sha256:0c567a8bd671331808b89a96976fabb305d57b123dec4fa62a20bfb89f2b32b9

ARG HADOOP_VERSION=3.4.3
ARG TARGETARCH

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

# Apache ships a native arm64 tarball, so no emulation on Apple Silicon.
RUN set -eux; \
    case "${TARGETARCH}" in arm64) suffix="-aarch64" ;; *) suffix="" ;; esac; \
    base="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}"; \
    file="hadoop-${HADOOP_VERSION}${suffix}.tar.gz"; \
    curl -fsSL "${base}/${file}" -o /tmp/hadoop.tgz; \
    expected="$(curl -fsSL "${base}/${file}.sha512" | grep -oE '[0-9a-fA-F]{128}' | head -1)"; \
    echo "${expected}  /tmp/hadoop.tgz" | sha512sum -c -; \
    mkdir -p "${HADOOP_HOME}"; \
    tar -xzf /tmp/hadoop.tgz -C "${HADOOP_HOME}" --strip-components=1; \
    rm /tmp/hadoop.tgz

RUN useradd --create-home --uid 1000 --shell /bin/bash hadoop \
 && mkdir -p /data/hdfs/name /data/hdfs/data \
 && chown -R hadoop:hadoop /data "${HADOOP_HOME}"

COPY --chown=hadoop:hadoop conf/ ${HADOOP_CONF_DIR}/
COPY --chmod=755 entrypoint.sh /usr/local/bin/entrypoint.sh

USER hadoop
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
