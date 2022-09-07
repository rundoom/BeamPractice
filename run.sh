#!/bin/bash


docker-compose --file external/docker-compose.yml up -d


./gradlew run --args=" \
--kafkaBootstrapServer=localhost:29092 \
--kafkaTopic=event_topic_proto \
--postgresUrl=jdbc:postgresql://localhost:5432/postgres \
--postgresUsername=postgres \
--postgresPassword=example \
--redisHost=localhost \
--redisPort=6379 \
"