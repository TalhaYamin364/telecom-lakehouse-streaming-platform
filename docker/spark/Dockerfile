# Spark 4.0.5 plus the Iceberg and Kafka connectors, each jar pinned by sha256 so a rebuild cannot change them.
FROM apache/spark:4.0.5-scala2.13-java17-python3-ubuntu@sha256:45eb1b8261a89bc7692be05aa4664676856f1c886cecc7b97b4e10f635b45b3c

ADD --checksum=sha256:63c3b812549dc232c1920865ff42436df8604a9bbacd693930985c06bd97e6dc \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar /opt/extra-jars/
ADD --checksum=sha256:01b7d9f88a3b8c7ea5420c5ef590a63766d46054440049c8cddd07eb9e77255d \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.5/spark-sql-kafka-0-10_2.13-4.0.5.jar /opt/extra-jars/
ADD --checksum=sha256:c3bb84a40564bebb5c21bdcd0d75687fcdfc419ce5aae03bd19e6707b01009a6 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.5/spark-token-provider-kafka-0-10_2.13-4.0.5.jar /opt/extra-jars/
ADD --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/extra-jars/
ADD --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/extra-jars/

USER root
RUN chmod -R a+rX /opt/extra-jars
COPY --chmod=755 submit-job.sh /opt/submit-job.sh
USER spark
