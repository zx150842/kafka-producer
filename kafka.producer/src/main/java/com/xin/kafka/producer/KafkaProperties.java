package com.xin.kafka.producer;

public interface KafkaProperties {
  
  String zkConnect = "";
  String groupId = "";
  String topic = "";
  String kafkaServerURL = "";
  int kafkaServerPort = 9092;
  int connectionTimeout = 20000;
  int reconnectInterval = 10000;
  String clientId = "";
}
