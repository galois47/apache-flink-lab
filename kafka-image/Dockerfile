FROM confluentinc/cp-kafka:7.5.0

# Copy entrypoint (boots Kafka and creates topics)
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]
