package com.demo.flinkiceberg.config;

public abstract class ConfigInfo {

    public final static String BOOTSTRAP_SERVERS_CONFIG = "b-2.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-3.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-1.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092";

    public static final String DEFAULT_KAFKA_TOPIC = "kafkatopic";

    public static final String FLINK_CHECKPOINT = "s3://sg-emr-flink-iceberg/checkpoint/ReadFromKafkaJob/";

    public static final String MYSQL_URL = "jdbc:mysql://user-database.crcwrov0yr1e.ap-southeast-1.rds.amazonaws.com/user_db";

    public static final String MYSQL_USER = "";

    public static final String MYSQL_PSW = "";

}
