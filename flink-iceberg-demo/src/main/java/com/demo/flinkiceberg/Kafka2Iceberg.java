package com.demo.flinkiceberg;


// ./bin/flink run -m yarn-cluster -ynm etlkafka -p 20 -ys 20 -yjm 1024 -ytm 10240  -c  com.demo.flinkiceberg.Kafka2Iceberg  /home/hadoop/flinkapp/etl/flink-iceberg-demo-1.0-SNAPSHOT.jar

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

import static com.demo.flinkiceberg.config.ConfigInfo.*;

public class Kafka2Iceberg {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setInteger("table.exec.resource.default-parallelism", 5);
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
        Properties properties = new Properties();
        properties.setProperty("max.partition.fetch.bytes", "10485760");
        properties.setProperty("request.timeout.ms", "120000");
        properties.setProperty("session.timeout.ms", "60000");
        properties.setProperty("heartbeat.interval.ms", "10000");
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(FLINK_CHECKPOINT);

        String kafkasourceTable = "CREATE TABLE IF NOT EXISTS default_database.kafka_table (\n" +
                "                             `webpageId` int,\n" +
                "                             `uid` STRING,\n" +
                "                             `productId` STRING,\n" +
                "                             `cookieId` STRING,\n" +
                "                             `expendTime` int,\n" +
                "                             `updateTime` BIGINT,\n" +
                "                             `proctime` as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "                             `eventTime` AS TO_TIMESTAMP(FROM_UNIXTIME(updateTime/1000, 'yyyy-MM-dd HH:mm:ss')) -- 事件时间\n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = '"+ DEFAULT_KAFKA_TOPIC +"',\n" +
                "      'properties.bootstrap.servers' = '"+ BOOTSTRAP_SERVERS_CONFIG +"',\n" +
                "      'properties.group.id' = 'test-group02',\n" +
                "      'scan.startup.mode' = 'earliest-offset',\n" +
                "      'format' = 'json'\n" +
                "      )";


        String productsTable = "CREATE TABLE IF NOT EXISTS default_database.products_jdbc (\n" +
                "          productId STRING PRIMARY KEY,\n" +
                "          name STRING,\n" +
                "          description STRING,\n" +
                "          product_price DECIMAL(10, 4),\n" +
                "          update_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'url' = '" + MYSQL_URL + "',\n" +
                "      'table-name' = 'products',\n" +
                "      'username' = '"+ MYSQL_USER + "',\n" +
                "      'password' = '"+ MYSQL_PSW +"',\n" +
                "      'scan.fetch-size' = '100',\n" +
                "      'lookup.cache.max-rows' = '5000',\n" +
                "      'lookup.cache.ttl' = '10s',\n" +
                "      'lookup.max-retries' = '3'\n" +
                "      )" ;


        String flinkCatalogSQL = "create catalog iceberg_hive_catalog with(\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'hive-conf-dir'='/usr/lib/hive/conf',\n" +
                "  'warehouse'='s3://sg-emr-flink-iceberg/mywarehouse/'\n" +
                ")";

        String clickEventTable = "create table IF NOT EXISTS ods_behavior.clickevent_v5(\n" +
                "    `webpageId` int,\n" +
                "    `uid` STRING,\n" +
                "    `productId` STRING,\n" +
                "    `cookieId` STRING,\n" +
                "    `expendTime` int,\n" +
                "    `updateTime` TIMESTAMP(3),\n" +
                "    `name` STRING,\n" +
                "    `product_price` DECIMAL(10, 4),\n" +
                "    `dt` STRING,\n" +
                "    `eventTime` TIMESTAMP(3)\n" +
                ") PARTITIONED BY (dt) with(\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                " 'write.metadata.delete-after-commit.enabled'='true',\n" +
                " 'write.metadata.previous-versions-max'='5',\n" +
                " 'sink.parallelism' = '5',\n" +
                " 'sink.partition-commit.policy.kind'='metastore,success-file', \n" +
                " 'warehouse'='s3://sg-emr-flink-iceberg/mywarehouse/',\n" +
                " 'write.upsert.enable'='true',\n" +
                " 'format-version'='2'\n" +
                ")";


        String insertETL = "insert into ods_behavior.clickevent_v5(\n" +
                "    webpageId,\n" +
                "    uid,\n" +
                "    productId,\n" +
                "    cookieId,\n" +
                "    expendTime,\n" +
                "    updateTime,\n" +
                "    name,\n" +
                "    product_price,\n" +
                "    eventTime,\n" +
                "    dt\n" +
                ") select\n" +
                "    aa.webpageId,\n" +
                "    aa.uid,\n" +
                "    aa.productId,\n" +
                "    aa.cookieId,\n" +
                "    aa.expendTime,\n" +
                "    aa.updateTime,\n" +
                "    bb.name,\n" +
                "    bb.product_price,\n" +
                "    aa.eventTime,\n" +
                "    DATE_FORMAT(LOCALTIMESTAMP, 'yyyyMMdd')\n" +
                "from\n" +
                "    default_catalog.default_database.kafka_table AS aa\n" +
                "        left join default_catalog.default_database.products_jdbc FOR SYSTEM_TIME AS OF aa.proctime AS bb\n" +
                "                  on aa.productId = bb.productId";

        tableEnv.executeSql(kafkasourceTable);
        tableEnv.executeSql(productsTable);
        tableEnv.executeSql(flinkCatalogSQL);
        tableEnv.useCatalog("iceberg_hive_catalog");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods_behavior");
        tableEnv.executeSql(clickEventTable);
        tableEnv.executeSql(insertETL);
    }
}
