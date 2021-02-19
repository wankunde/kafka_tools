// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package com.wankun.tools

import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-18.
 * 工具类，负责消费指定kafka topic
 */
object KafkaTools extends KafkaUtils[String, String] with AutoCloseable {

  def main(args: Array[String]): Unit = {
    parseArgument(args)
    val earliestPartitionsMap = fetchEarlyOffsets(getPartitions().asJava)
    val latestPartitionsMap = fetchLatestOffsets(getPartitions().asJava)
    println(
      s"""Earliest Offset Info:
         |${offsetMapToString(earliestPartitionsMap)}
         |Latest Offset Info:
         |${offsetMapToString(latestPartitionsMap)}
         |""".stripMargin)

    pollMessageForTime(startTs, endTs, CallBack.consolePrint)
  }

  def parseArgument(args: Array[String]): Unit = {
    var i = 0
    while (i < args.length) {
      if (args(i) == "--topic") {
        i = i + 1
        topic = args(i)
      } else if (args(i) == "--bootstrap-server") {
        i = i + 1
        bootstrapServer = args(i)
      } else if (args(i) == "--messageType") {
        i = i + 1
        messageType = args(i)
      } else if (args(i) == "--schemaRegistry") {
        i = i + 1
        schemaRegistry = args(i)
      } else if (args(i) == "--startTs") {
        i = i + 1
        startTs = args(i).toLong
      } else if (args(i) == "--endTs") {
        i = i + 1
        endTs = args(i).toLong
      }
      i = i + 1
    }

    assert(StringUtils.isNotBlank(topic), "kafka topic should not be null!")
    assert(StringUtils.isNotBlank(bootstrapServer), "kafka brokers should not be null!")
  }

  override def close(): Unit = {
    closeConsumer()
  }
}
