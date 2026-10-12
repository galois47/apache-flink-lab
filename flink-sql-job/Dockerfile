FROM flink:1.18.1-java11

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar

COPY script.sql /opt/flink/script.sql

WORKDIR /opt/flink

CMD ["bash", "-c", "sleep 20 && /opt/flink/bin/sql-client.sh -f script.sql"]
