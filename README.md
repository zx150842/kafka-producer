Kafka-producer is used as a click log producer which can watch log file change and send new logs to kafka cluster.

Kafka-producer has two major parts : watch file change and send changes to kafka cluster.

1 Watch file change:
1.1 Support file log segmentation.
1.2 Support watch a directory and all its children recursively.
1.3 Support a thread monitor which can recover the not alive producer.

2 Send changes to kafka cluster:
2.1 Support breakpoint transmission using checkpoint.
2.2 Support checkpoint persistence.
