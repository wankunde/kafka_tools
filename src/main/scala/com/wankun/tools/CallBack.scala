// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package com.wankun.tools

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-19.
 */
object CallBack {

  def consolePrint[K, V](r: ConsumerRecord[K, V]): Unit = {
    println(s"offset: ${r.offset} ${r.timestamp()} ${r.key} -> ${r.value} ")
  }
}
