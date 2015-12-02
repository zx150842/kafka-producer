Kafka-producer is used as a click log producer which can watch log file change and send new logs to kafka cluster.

Kafka-producer has two major parts : watch file change and send changes to kafka cluster.
<h4>1 Watch file change:</h4>
1.1 Support file log segmentation.<br>
1.2 Support watch a directory and all its children recursively.<br>
1.3 Support a thread monitor which can recover the not alive producer.<br>

<h4>2 Send changes to kafka cluster:</h4>
2.1 Support breakpoint transmission using checkpoint.<br>
2.2 Support checkpoint persistence.<br>
