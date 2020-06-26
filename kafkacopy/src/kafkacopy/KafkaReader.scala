// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.time.Duration
import java.util.Collections
import java.util.Collections.singletonList

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

class KafkaReader(private val consumer: Consumer[Array[Byte], Array[Byte]], private val timeout: Duration) {
  import scala.jdk.CollectionConverters._
  private val log = LoggerFactory.getLogger(classOf[KafkaReader])

  def listTopics(): List[String] =
    consumer.listTopics(timeout).asScala.toList.flatMap(_._2.asScala.toList.map(_.topic())).distinct.sorted

  def listTopicPartitions(): List[TopicPartitionDto] =
    consumer
      .listTopics(timeout)
      .asScala
      .toList
      .flatMap(_._2.asScala.toList.map { pi =>
        TopicPartitionDto(pi.topic(), pi.partition())
      })
      .sorted

  def listTopicPartitions(topic: String): List[TopicPartitionDto] =
    consumer
      .partitionsFor(topic)
      .asScala
      .toList
      .map { pi =>
        TopicPartitionDto(pi.topic(), pi.partition())
      }
      .sorted

  def offsets(topicPartitions: List[TopicPartitionDto]): List[OffsetDto] = {
    val tp = topicPartitions.map(tp => new TopicPartition(tp.topic, tp.partition)).asJava
    consumer.assign(tp)
    val begin = consumer.beginningOffsets(tp, timeout).asScala
    val end = consumer.endOffsets(tp, timeout).asScala
    begin
      .map {
        case (tp, off) =>
          OffsetDto(tp.topic(), tp.partition(), off, end(tp))
      }
      .toList
      .sorted
  }

  def readAll(topicPartitions: List[TopicPartitionDto]): Vector[RecordDto] = {
    var replies: Vector[RecordDto] = Vector.empty[RecordDto]
    read(topicPartitions, { recs =>
      replies ++= recs
    })
    replies
  }

  def read(topicPartitions: List[TopicPartitionDto], batch: Vector[RecordDto] => Unit): Unit = {
    val parts = topicPartitions.map { tp =>
      CopyRange(TopicPartitionDto(tp.topic, tp.partition), First, Last)
    }.toSet
    readTopicPartitions(parts, batch)
  }

  def readTopicPartitions(src: Set[CopyRange], batch: Vector[RecordDto] => Unit): Unit = {

    val partMap: Map[TopicPartition, CopyRange] = src.map { p =>
      (new TopicPartition(p.topicPartition.topic, p.topicPartition.partition), p)
    }.toMap

    val topicPartitions = partMap.keySet.toList.asJava

    lazy val beginningOffsets: Map[TopicPartition, Long] = {
      log.debug("beginningOffsets")
      consumer.assign(topicPartitions)
      consumer
        .beginningOffsets(topicPartitions, timeout)
        .asScala
        .map { case (k, v) => (k, v.toLong) }
        .toMap
    }
    lazy val endOffsets: Map[TopicPartition, Long] = {
      log.debug("endOffsets")
      consumer.assign(topicPartitions)
      consumer
        .endOffsets(topicPartitions, timeout)
        .asScala
        .map { case (k, v) => (k, v.toLong) }
        .toMap
    }

    def fistOffset(tp: TopicPartition): Option[Long] = {
      log.debug("fistOffset")
      val offset = beginningOffsets(tp)
      consumer.assign(singletonList(tp))
      consumer.seek(tp, offset)
      consumer.poll(timeout).asScala.headOption.map(_.offset())
    }
    def lastOffset(tp: TopicPartition): Option[Long] = {
      log.debug("lastOffset")
      val offset = endOffsets(tp)
      consumer.assign(singletonList(tp))
      consumer.seek(tp, offset)
      findLastOffset(tp, endOffsets(tp) - 1)
    }
    @tailrec
    def findLastOffset(tp: TopicPartition, off: Long): Option[Long] = {
      if (off < beginningOffsets(tp)) None
      else {
        consumer.seek(tp, off)
        val lastOffset = consumer.poll(timeout).asScala.lastOption.map(_.offset())
        lastOffset match {
          case None => findLastOffset(tp, off - 1)
          case _    => lastOffset
        }
      }
    }

    def resolvePosition(topicPartition: TopicPartition, position: Position): Option[Long] = position match {
      case Beginning      => Some(beginningOffsets(topicPartition))
      case End            => Some(endOffsets(topicPartition))
      case First          => fistOffset(topicPartition)
      case Last           => lastOffset(topicPartition)
      case Offset(offset) => Some(offset)
    }

    val fromOffsets: Map[TopicPartition, Option[Long]] = partMap.map {
      case (tp, part) =>
        (tp, resolvePosition(tp, part.from))
    }

    val toOffsets: Map[TopicPartition, Option[Long]] = partMap.map {
      case (tp, part) =>
        (tp, resolvePosition(tp, part.to))
    }

    consumer.assign(topicPartitions)

    val starts: Map[TopicPartition, Long] = fromOffsets.collect { case (k, Some(v)) => (k, v) }
    val ends: Map[TopicPartition, Long] = toOffsets.collect { case (k, Some(v))     => (k, v) }
    var pendingPartitions: Set[TopicPartition] = starts.keySet.union(ends.keySet)
    @tailrec
    def fetch(): Unit = {
      log.debug("polling")
      val results = consumer.poll(timeout)
      val pendingPartitionsPrime = pendingPartitions
      val filtered = results.asScala.toVector.filter { rec =>
        val tp = new TopicPartition(rec.topic(), rec.partition())
        val end = ends(tp)
        if (rec.offset() == end) {
          pendingPartitions -= tp
        }
        rec.offset() <= end
      }
      if (filtered.nonEmpty)
        batch(filtered.map(Record.consumerRecordToDto))
      if (pendingPartitions.nonEmpty) {
        if (pendingPartitionsPrime != pendingPartitions) {
          consumer.assign(partMap.keySet.intersect(pendingPartitions).toList.asJava)
        }
        fetch()
      }
    }
    starts.foreach {
      case (partition, start) =>
        consumer.seek(partition, start)

    }
    fetch()
    consumer.assign(Collections.emptyList())
    log.debug("done")
    batch(Vector.empty)
  }
}
