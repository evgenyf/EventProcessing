package com.gs.poc.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Properties;

public class TPriEventConsumer extends AbstractConsumer{

    public TPriEventConsumer(String topic, Properties kafkaProps ){
        super(topic, kafkaProps, KafkaAvroDeserializer.class.getName() /*TPriEventDeserializer.class.getName()*/, "http://localhost:8081");
    }

/*    @Override
    protected void readFromKafka() {
        try {
            //ConsumerRecords<String, TPriEvent> records = kafkaConsumer.poll(Duration.ofMillis(100));
            ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, GenericRecord> consumerRecord : records) {
                System.out.println("Message received:" + ", consumerRecord:" + consumerRecord );
                TPriEvent event = mapRecordToObject(consumerRecord.value(), new TPriEvent());
                System.out.println("TYPE:" + event.getType() + " , EVENT=" + event + ", consumerRecord:" + consumerRecord );
            }
        }
        catch( Exception e ){
            e.printStackTrace();
        }
    }*/

    @Override
    protected void readFromKafka() {
        try {
            ConsumerRecords<String, TPriEvent> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, TPriEvent> consumerRecord : records) {
                TPriEvent tpriEvent = consumerRecord.value();
                System.out.println("Message received, TYPE:" + tpriEvent.getType() + ", tpriEvent=" + tpriEvent + ", consumerRecord:" + consumerRecord );
            }
        }
        catch( Exception e ){
            e.printStackTrace();
        }
    }

    private static <T> T mapRecordToObject(GenericRecord record, T object) {
        Assert.notNull(record, "record must not be null");
        Assert.notNull(object, "object must not be null");
        final Schema schema = ReflectData.get().getSchema(object.getClass());

        System.out.println( "SCHEMA FIELDS1=" + schema.getFields() );
        System.out.println( "SCHEMA FIELDS2=" + record.getSchema().getFields() );

        //Assert.isTrue(schema.getFields().equals(record.getSchema().getFields()), "Schema fields didn’t match");

        record.getSchema().getFields().forEach(d -> PropertyAccessorFactory.forDirectFieldAccess(object).setPropertyValue(d.name(), record.get(d.name()) == null ? record.get(d.name()) : record.get(d.name()).toString()));
        return object;
    }
}