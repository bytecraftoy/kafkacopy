// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.time.Duration
import java.util.Collections.singletonList

import org.apache.kafka.clients.admin.{Admin, CreateTopicsOptions, DeleteTopicsOptions, NewTopic}

class KafkaAdmin(private val admin: Admin, private val timeout: Duration) {

  def crateTopic(
      name: String,
      partitions: Option[Int] = None,
      replicationFactor: Option[Int] = None,
      configs: Map[String, String] = Map()
  ): Unit = {
    import scala.jdk.CollectionConverters._
    import scala.jdk.OptionConverters._
    admin
      .createTopics(
        singletonList(
          new NewTopic(
            name,
            partitions.map(java.lang.Integer.valueOf).toJava,
            replicationFactor.map(x => java.lang.Short.valueOf(x.toShort)).toJava
          ).configs(configs.asJava)
        ),
        new CreateTopicsOptions().timeoutMs(timeout.toMillis.toInt)
      )
      .all()
      .get()
    ()
  }

  def deleteTopic(name: String): Unit = {
    admin
      .deleteTopics(singletonList(name), new DeleteTopicsOptions().timeoutMs(timeout.toMillis.toInt))
      .all()
      .get()
    ()
  }
}
