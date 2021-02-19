# 说明

本工具是为了作为 `kafka-console-consumer.sh` 的功能补充工具，在不影响现有的kafka各个消费topic的前提下，提供了消费指定时间段的消息的功能。

* 消费的消息类型支持 `string` 和 `avro` 两种类型

使用样例:

`java -cp ./kafka_tools-1.0.jar com.wankun.tools.KafkaTools --topic topic --bootstrap-server 192.168.1.1:9092,192.168.1.2:9092 --startTs 1613583073000 --endTs  1613584873000`

