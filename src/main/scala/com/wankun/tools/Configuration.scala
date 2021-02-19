// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package com.wankun.tools

import org.apache.kafka.common.TopicPartition

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-19.
 */
trait Configuration {

  type PartitionAndOffsets = Map[TopicPartition, Long]

  var topic = ""
  var bootstrapServer = ""
  var schemaRegistry = ""
  var messageType = "string"
  var startTs: Long = 0
  var endTs: Long = 0

  var pollTimeoutSeconds: Int = 10

}
