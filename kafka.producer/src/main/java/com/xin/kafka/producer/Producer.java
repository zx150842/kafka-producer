package com.xin.kafka.producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xin.kafka.util.Resources;

public class Producer {

  static final Logger log = LoggerFactory.getLogger(Producer.class);
  
  private KafkaProducer<String, String> kafkaProducer;
  
  private final String producerConfigPath = "producer.properties";
  private final String topic;
  
  Producer(String topic) {
    this.topic = topic;
    try {
      Properties properties = Resources.getResourceAsProperties(producerConfigPath);
      kafkaProducer = new KafkaProducer<String, String>(properties);
    } catch (IOException e) {
      log.error("[Producer]Cannot create kafkaProducer, topic : {} ", topic, e);
    }
  }
  
  public boolean send(List<String> records) {
    final AtomicInteger succCount = new AtomicInteger(0);
    final long startTime = System.currentTimeMillis();
    for (final String record : records) {
      log.info("[Producer]Producer is sending {} to kafka. ", record);
      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, record);
      kafkaProducer.send(producerRecord, new Callback() {
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (null == exception) {
            log.info("[Producer]Succ send {} to kafka. ", record);
            succCount.incrementAndGet();
          } else {
            log.error("[Producer]Cannot send {} to kafka, error : {} ", record, exception);
          }
        }
      });
    }
    while (true) {
      if (succCount.get() == records.size()) {
        break;
      }
      long now = System.currentTimeMillis();
      if (now - startTime > 10000) {
        log.warn("[Producer]Timeout before recevie response from kafka.");
        break;
      }
    }
    return succCount.get() == records.size();
  }
  
  public void close() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
    }
  }
}
