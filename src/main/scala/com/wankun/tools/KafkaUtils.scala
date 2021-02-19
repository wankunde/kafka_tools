// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package com.wankun.tools

import java.time.Duration
import java.util.{Collections, Properties}
import java.{lang => jl, util => ju}

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-18.
 */
trait KafkaUtils[K, V] extends Configuration {



  @volatile protected var _consumer: Consumer[K, V] = null

  def getOrCreateConsumer(): Consumer[K, V] = synchronized {
    if (_consumer == null) {
      val props = new Properties()
      props.put("schema.registry.url", schemaRegistry)
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
      if ("string".equalsIgnoreCase(messageType)) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          classOf[StringDeserializer].getCanonicalName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          classOf[StringDeserializer].getCanonicalName)
      } else if ("avro".equalsIgnoreCase(messageType)) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          classOf[KafkaAvroDeserializer].getCanonicalName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          classOf[KafkaAvroDeserializer].getCanonicalName)
      }

      _consumer = new KafkaConsumer[K, V](props)
    }
    _consumer
  }


  def withConsumer[T](body: Consumer[K, V] => T): T = {
    val consumer = getOrCreateConsumer()
    body(consumer)
  }

  def closeConsumer(): Unit = withConsumer { consumer =>
    consumer.close()
  }

  def offsetMapToString(partitionsMap: PartitionAndOffsets): String = {
    partitionsMap.map(p => s"${p._1} : ${p._2}").mkString("\n")
  }

  def getPartitions(): Seq[TopicPartition] = {
    val consumer = getOrCreateConsumer()
    consumer.partitionsFor(topic).asScala
      .map(p => new TopicPartition(p.topic(), p.partition()))
  }

  def fetchEarlyOffsets(partitions: ju.List[TopicPartition]): PartitionAndOffsets = {
    val consumer = getOrCreateConsumer()
    consumer.assign(partitions)
    consumer.seekToBeginning(partitions)
    partitions.asScala.map(p => p -> consumer.position(p)).toMap
  }

  def fetchLatestOffsets(partitions: ju.List[TopicPartition]): PartitionAndOffsets = {
    val consumer = getOrCreateConsumer()
    consumer.assign(partitions)
    consumer.seekToEnd(partitions)
    partitions.asScala.map(p => p -> consumer.position(p)).toMap
  }

  def collectMessageForOffsets(partitionsMap: PartitionAndOffsets,
                               rowLimit: Int = 100,
                               callback: ConsumerRecord[K, V] => Unit)
  : Unit = {
    val consumer = getOrCreateConsumer()
    partitionsMap.foreach { case (partition, offset) =>
      consumer.seek(partition, offset)
    }
    var i = 0
    while (i < rowLimit) {
      val records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds))
      val it = records.iterator()
      // empty result in pollTimeoutSeconds seconds, just return
      if (!it.hasNext) {
        return
      }
      while (i < rowLimit && it.hasNext) {
        val msg = it.next()
        i = i + 1
        callback(msg)
      }
    }
  }

  def pollMessageForTime(start: jl.Long,
                         end: jl.Long,
                         callback: ConsumerRecord[K, V] => Unit): Unit = {
    val consumer = getOrCreateConsumer()
    val partitions = getPartitions()
    val startOffsetsMap: mutable.Map[TopicPartition, OffsetAndTimestamp] =
      consumer.offsetsForTimes(partitions.map(tp => tp -> start).toMap.asJava).asScala
    val endOffsetsMap: mutable.Map[TopicPartition, OffsetAndTimestamp] =
      consumer.offsetsForTimes(partitions.map(tp => tp -> end).toMap.asJava).asScala

    for (partition <- startOffsetsMap.keys ++ endOffsetsMap.keys) {
      // 看上去返回的offset是在指定时间之后的最近一个offset，如果这个offset为空，使用earliest offset或 latest offset 替代
      val startOffset =
        startOffsetsMap(partition) match {
          case ts if ts != null => ts.offset()
          case _ => fetchEarlyOffsets(Collections.singletonList(partition))(partition)
        }
      val endOffset =
        endOffsetsMap(partition) match {
          case ts if ts != null => ts.offset()
          case _ => fetchLatestOffsets(Collections.singletonList(partition))(partition)
        }
      fetchMessageFromPartition(partition, startOffset, endOffset, callback)
    }
  }

  def fetchMessageFromPartition(partition: TopicPartition,
                                startOffset: Long,
                                endOffset: Long,
                                callback: ConsumerRecord[K, V] => Unit): Unit = {
    val consumer = getOrCreateConsumer()
    consumer.assign(Collections.singleton(partition))
    consumer.seek(partition, startOffset)
    var flag = true
    while (flag) {
      val records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds))
      val it = records.iterator()
      // empty result in pollTimeoutSeconds seconds, just return
      if (!it.hasNext) {
        return
      }
      while (it.hasNext) {
        val msg = it.next()
        if (msg.offset() < endOffset) {
          callback(msg)
        } else {
          flag = false
        }
      }
    }
  }
}
