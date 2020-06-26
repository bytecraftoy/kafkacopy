// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import org.apache.kafka.clients.producer.Producer

object KafkaWriter {
  def mkKafkaWriter(producer: Producer[Array[Byte], Array[Byte]]): Vector[RecordDto] => Unit = { xs =>
    xs.foreach(x => producer.send(Record.dtoToProducerRecord(x)))
    if (xs.isEmpty) producer.flush()
  }
}
