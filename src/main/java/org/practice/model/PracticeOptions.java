package org.practice.model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface PracticeOptions extends PipelineOptions {
    @Description("kafkaBootstrapServer")
    @Default.String("Nothing")
    String getKafkaBootstrapServer();
    void setKafkaBootstrapServer(String value);

    @Description("kafkaTopic")
    @Default.String("Nothing")
    String getKafkaTopic();
    void setKafkaTopic(String value);

    @Description("postgresUrl")
    @Default.String("Nothing")
    String getPostgresUrl();
    void setPostgresUrl(String value);

    @Description("postgresUsername")
    @Default.String("Nothing")
    String getPostgresUsername();
    void setPostgresUsername(String value);

    @Description("postgresPassword")
    @Default.String("Nothing")
    String getPostgresPassword();
    void setPostgresPassword(String value);

    @Description("redisHost")
    @Default.String("Nothing")
    String getRedisHost();
    void setRedisHost(String value);

    @Description("redisPort")
    @Default.Integer(1234)
    Integer getRedisPort();
    void setRedisPort(Integer value);
}