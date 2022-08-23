package com.demo.gendata;

import cn.hutool.core.date.DateTime;
import com.alibaba.fastjson2.JSON;
import com.demo.flinkiceberg.warshouse.bean.ClickEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * java -cp gendata-1.0-SNAPSHOT-jar-with-dependencies.jar com.demo.gendata.DataGen2 -c 100000 -s 10
 */

public class DataGen2 {

    static class SampleCmdOption {
        @Option(name = "-c", usage = "mock data number")
        public int count = 1000000;

        @Option(name = "-s", usage = "stop time /10")
        public long sleeptime = 1L;

        @Option(name = "-bts", usage = "kafka bootstrap")
        public String bootstrap;
    }

    private final static String BOOTSTRAP_SERVERS_CONFIG = "b-2.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-3.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-1.clieck-stream.25k87w.c5.kafka.ap-southeast-1.amazonaws.com:9092";

    private static final String DEFAULT_KAFKA_TOPIC = "kafkatopic";

    private final static Map<Integer, String> USERINFR_MAP = new HashMap<Integer, String>() {{
        put(1, "513248");
        put(2, "10952");
        put(3, "555655");
        put(4, "795098");
        put(5, "603670");
    }};


    public static void main(String[] args) {
        SampleCmdOption option = new SampleCmdOption();
        CmdLineParser parser = new CmdLineParser(option);
        try {
            if (args.length == 0) {
                showHelp(parser);
                return;
            }
            parser.parseArgument(args);
            System.out.println(option.count);
            System.out.println(option.sleeptime);
        } catch (CmdLineException cle) {
            System.out.println("Command line error: " + cle.getMessage());
            showHelp(parser);
            return;
        } catch (Exception e) {
            System.out.println("Error in main: " + e.getMessage());
            e.printStackTrace();
            return;
        }


        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("acks", "-1");
        properties.put("batch.size", "1048576");
        properties.put("linger.ms", "5");
        //properties.put("compression.type", "snappy");
        properties.put("buffer.memory", "33554432");
        properties.put("client.id", "producer.client.id.demo");

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        System.out.println("开始发送数据");

        // 4. 调用 send 方法,发送消息
        for (int i = 0; i < option.count; i++) {
            String value = JSON.toJSONString(genUserBehavior());
            ProducerRecord<String, String> record = new ProducerRecord<>(DEFAULT_KAFKA_TOPIC, value);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition() + ":" + metadata.offset() + ":" + value);
                }
            });

            if (i % 1000 == 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(option.sleeptime);
                    kafkaProducer.flush();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        kafkaProducer.flush();
        // 5. 关闭资源
        kafkaProducer.close();
    }

    static ClickEvent genUserBehavior() {
        ClickEvent clickEvent = new ClickEvent();
        clickEvent.setCookieId(RandomStringUtils.random(15, true, false));
        clickEvent.setExpendTime(RandomUtils.nextInt(0, 20));
        clickEvent.setWebpageId(RandomUtils.nextInt(10, 100));
        clickEvent.setUid(USERINFR_MAP.getOrDefault(RandomUtils.nextInt(1, 6), "10001"));
        clickEvent.setUpdateTime(System.currentTimeMillis());
        clickEvent.setUpdateTime(DateTime.now().getTime());
        clickEvent.setProductId(String.valueOf(RandomUtils.nextInt(0,20)));
        return clickEvent;
    }

    public static void showHelp(CmdLineParser parser) {
        System.out.println("LDA [options ...] [arguments...]");
        parser.printUsage(System.out);
    }
}
