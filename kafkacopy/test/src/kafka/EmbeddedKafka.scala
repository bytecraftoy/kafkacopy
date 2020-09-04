// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import org.springframework.kafka.test.EmbeddedKafkaBroker

object EmbeddedKafka {

  def withEmbeddedKafka(count: Int, controlledShutdown: Boolean, partitions: Int, topics: String*)(
      f: String => Unit): Unit = {
    val broker = new EmbeddedKafka(count, controlledShutdown, partitions, topics: _*)
    broker.start()
    try {
      f(broker.getBrokersAsString)
    } finally {
      broker.close()
    }
  }
}

class EmbeddedKafka(private val count: Int,
                    private val controlledShutdown: Boolean,
                    private val partitions: Int,
                    private val topics: String*)
    extends AutoCloseable {

  private[this] val broker = new EmbeddedKafkaBroker(count, controlledShutdown, partitions, topics: _*)

  def start(): Unit = broker.afterPropertiesSet()

  def getBrokersAsString: String = broker.getBrokersAsString

  override def close(): Unit = broker.destroy()

}
