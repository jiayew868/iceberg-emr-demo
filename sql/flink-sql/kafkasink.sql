
-- kafka table--

CREATE TABLE kafka_table (
        webpageId int,
        `uid` STRING,
        `productIds` STRING,
        `cookieId` STRING,
        `expendTime` int,
        `updateTime` bigint
) WITH (
      'connector' = 'kafka',
      'topic' = 'clicktopic',
      'properties.bootstrap.servers' = 'b-2.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-3.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-1.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092',
      'properties.group.id' = 'flink-test-group',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json'
      )

create catalog iceberg_hive_catalog with(
				  'type'='iceberg',
				  'catalog-type'='hive',
				  'clients'='5',
				  'property-version'='1',
				   'hive-conf-dir'='/usr/lib/hive/conf',
				  'warehouse'='s3://sg-emr-flink-iceberg/mywarehouse/'
				);

use catalog iceberg_hive_catalog;

CREATE DATABASE IF NOT EXISTS ods_behavior;

create table IF NOT EXISTS ods_behavior.clickevent(
                                                      `webpageId` int,
                                                      `uid` STRING,
                                                      `productIds` STRING,
                                                      `cookieId` STRING,
                                                      `updateTime` timestamp
)with(
     'type'='iceberg',
     'catalog-type'='hive',
     'write.metadata.delete-after-commit.enabled'='true',
     'write.metadata.previous-versions-max'='5',
     'warehouse'='s3://sg-emr-flink-iceberg/mywarehouse/',
     'write.upsert.enable'='true',
     'format-version'='2'
     );



--- 08.17-----

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE kafka_table (
                             `webpageId` int,
                             `uid` STRING,
                             `productId` STRING,
                             `cookieId` STRING,
                             `expendTime` int,
                             `updateTime` BIGINT,
                             `proctime` as PROCTIME(),   -- 通过计算列产生一个处理时间列
                             `eventTime` AS TO_TIMESTAMP(FROM_UNIXTIME(updateTime/1000, 'yyyy-MM-dd HH:mm:ss')) -- 事件时间
) WITH (
      'connector' = 'kafka',
      'topic' = 'kafkatopic',
      'properties.bootstrap.servers' = 'b-1.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-2.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-3.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092',
      'properties.group.id' = 'group01',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
      )


CREATE TABLE default_database.products_jdbc (
                                                productId STRING PRIMARY KEY,
                                                name STRING,
                                                description STRING,
                                                product_price DECIMAL(10, 4),
                                                update_time TIMESTAMP(3)
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://user-database.crcwrov0yr1e.ap-southeast-1.rds.amazonaws.com/user_db',
      'table-name' = 'products',
      'username' = 'admin',
      'password' = '12345678',
      'scan.fetch-size' = '100',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '10s',
      'lookup.max-retries' = '3'
      );




CREATE TABLE default_database.products (
                                           productId STRING,
                                           name STRING,
                                           description STRING,
                                           product_price DECIMAL(10, 4),
                                           update_time TIMESTAMP(3),
                                           PRIMARY KEY (productId) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = 'user-database.crcwrov0yr1e.ap-southeast-1.rds.amazonaws.com',
      'port' = '3306',
      'username' = 'admin',
      'password' = '12345678',
      'database-name' = 'user_db',
      'table-name' = 'products'
      );



select
    aa.webpageId,
    aa.uid,
    aa.productId,
    aa.cookieId,
    aa.expendTime,
    aa.updateTime,
    bb.name,
    bb.product_price,
    aa.eventTime
from
    default_catalog.default_database.kafka_table AS aa
        left join default_catalog.default_database.products_jdbc FOR SYSTEM_TIME AS OF aa.proctime AS bb
                  on aa.productId = bb.productId







CREATE TABLE dim_db.products_sink_v3(
                                        `id` STRING,
                                        `name`    STRING,
                                        `description`         STRING,
                                        product_price DECIMAL(10, 4),
                                        update_time TIMESTAMP(3),
                                        PRIMARY KEY(id) NOT ENFORCED
) with(
      'type'='iceberg',
      'catalog-type'='hive',
      'write.metadata.delete-after-commit.enabled'='true',
      'write.metadata.previous-versions-max'='5',
      'warehouse'='s3://sg-emr-flink-iceberg/mywarehouse/products_sink_v3',
      'format-version'='2'
      );

select productId, name, product_price, count(uid)
from ods_behavior.clickevent_v5 /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
group by productId