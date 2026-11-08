# Java 17 on Ubuntu 22.04, pinned by digest; Spark 4 executors run in the NodeManager's JVM and need 17.
# It already ships curl, tar and CA certificates.
FROM eclipse-temurin:17-jre-jammy@sha256:2a9eb226abb654c9d1f91b53cf638d20cb3abb726d1b18a67909c29515545469

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
