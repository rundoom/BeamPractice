package org.practice.model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PracticeOptions extends PipelineOptions {
    @Description("kafkaBootstrapServer")
    @Default.String("Nothing")
    ValueProvider<String> getKafkaBootstrapServer();
    void setKafkaBootstrapServer(ValueProvider<String> value);

    @Description("kafkaTopic")
    @Default.String("Nothing")
    ValueProvider<String> getKafkaTopic();
    void setKafkaTopic(ValueProvider<String> value);

    @Description("postgresUrl")
    @Default.String("Nothing")
    ValueProvider<String> getPostgresUrl();
    void setPostgresUrl(ValueProvider<String> value);

    @Description("postgresUsername")
    @Default.String("Nothing")
    ValueProvider<String> getPostgresUsername();
    void setPostgresUsername(ValueProvider<String> value);

    @Description("postgresPassword")
    @Default.String("Nothing")
    ValueProvider<String> getPostgresPassword();
    void setPostgresPassword(ValueProvider<String> value);

    @Description("redisHost")
    @Default.String("Nothing")
    ValueProvider<String> getRedisHost();
    void setRedisHost(ValueProvider<String> value);

    @Description("redisPort")
    @Default.Integer(1234)
    ValueProvider<Integer> getRedisPort();
    void setRedisPort(ValueProvider<Integer> value);
}