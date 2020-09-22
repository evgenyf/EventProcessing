package com.gs.poc.kafka;

import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class KafkaMain {

    static int i= 0;

    public static void main( String[] args ){

        String topic = "mcapi.avro.pri.processed";

        int kafkaPort = 9092;

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);

        TPriEventConsumer TPriEventConsumer = new TPriEventConsumer( topic, kafkaProps );
        TPriEventConsumer.startReadingFromKafka();
    }
}