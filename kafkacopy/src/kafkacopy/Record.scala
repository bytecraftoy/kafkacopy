// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders

object Record {
  import scala.jdk.CollectionConverters._

  def consumerRecordToDto(record: ConsumerRecord[Array[Byte], Array[Byte]]): RecordDto =
    RecordDto(
      record.topic(),
      Option(record.partition()),
      Option(record.offset()),
      record.headers().asScala.map(h => HeaderDto(h.key(), Option(h.value()))).toList,
      Option(record.key()),
      Option(record.value()),
      Option(record.timestamp())
    )

  def dtoToProducerRecord(record: RecordDto): ProducerRecord[Array[Byte], Array[Byte]] =
    new ProducerRecord[Array[Byte], Array[Byte]](
      record.topic,
      record.partition.map(java.lang.Integer.valueOf).orNull,
      record.timestamp.map(java.lang.Long.valueOf).orNull,
      record.key.orNull,
      record.value.orNull,
      record.headers.foldLeft(new RecordHeaders()) { (hdrs, h) =>
        hdrs.add(h.key, h.value.orNull)
        hdrs
      }
    )
}
