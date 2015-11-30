package com.xin.kafka.producer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.xin.kafka.util.Resources;

public class ProducerFactory {
  
  static final Logger log = LoggerFactory.getLogger(ProducerFactory.class);
  
  private static final String topicPath = "topic.properties";
  private static final Map<String, Producer> producerMap = Maps.newHashMap();
  
  static {
    try {
      Properties properties = Resources.getResourceAsProperties(topicPath);
      String topics = properties.getProperty("topics");
      if (topics != null) {
        for (String topic : topics.split(",")) {
          producerMap.put(topic, new Producer(topic));
        }
      }
    } catch (IOException e) {
      log.error("Cannot load topic from path : {}, error : {}", topicPath, e);
    }
  }
  
  private ProducerFactory() {
  }
  
  public static Producer getProducer(String topic) {
    if (producerMap.containsKey(topic)) {
      return producerMap.get(topic);
    }
    log.info("Cannot find producer for topic : {}", topic);
    return null;
  }
}
