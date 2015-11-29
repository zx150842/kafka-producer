package com.xin.kafka.producer;

import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer<K, V> {

  private KafkaProducer<K, V> kafkaProducer;
  
  public void send(List<String> records) {
    
  }
  
  public void send(ProducerRecord<K, V> record) {
    kafkaProducer.send(record);
  }
}
