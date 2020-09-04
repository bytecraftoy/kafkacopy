// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{InputStream, OutputStream, PrintWriter, Reader}
import java.nio.charset.Charset
import java.nio.file.{Files, Path}

import org.apache.commons.io.input.ReaderInputStream
import org.apache.commons.io.output.WriterOutputStream

class KafkaCopy(private val config: Cli.Config, stdin: Reader, stdout: PrintWriter) {

  private[this] val jsonMapper = new RecordJsonMapper()
  private[this] var fileOutputStream: Option[OutputStream] = None
  private[this] var fileInputStream: Option[InputStream] = None
  private[this] val cfgHome = new ConfigHome(config)
  private[this] val kafka = new Kafka(config, cfgHome)

  def close(): Unit = {
    kafka.close()
    fileInputStream.foreach(_.close)
    fileInputStream = None
    fileOutputStream.foreach(_.close)
    fileOutputStream = None
    stdout.flush()
  }

  def execute(): Unit = {
    try {
      config.cmd match {
        case Some(command) =>
          command match {
            case Cli.LsBrokers                           => kafka.listBrokers(stdout)
            case Cli.LsTopics(broker)                    => kafka.listTopics(broker, stdout)
            case Cli.LsPartitions(broker, topic)         => kafka.listPartitions(broker, topic, stdout)
            case Cli.LsOffsets(broker, topic, partition) => kafka.listOffsets(broker, topic, partition, stdout)
            case Cli.MkTopic(broker, topic)              => kafka.createTopic(broker, topic)
            case Cli.RmTopic(broker, topic)              => kafka.deleteTopic(broker, topic)
            case Cli.Copy                                => doCopy()
          }
        case None => ()
      }
    } finally {
      close()
    }
  }

  private def doCopy(): Unit = {
    val src = config.srcAddress.get
    val dst = config.dstAddress.get
    val dstPartition = dst match {
      case b: Cli.Broker => b.partition
      case _             => None
    }
    val dstTopic = dst match {
      case b: Cli.Broker => Option(b.topic)
      case _             => None
    }

    val adapt: RecordDto => RecordDto = { rec =>
      config.dstFields.foldLeft(RecordDto(topic = dstTopic.getOrElse(rec.topic), partition = dstPartition)) { (r, f) =>
        f match {
          case Cli.Key       => r.copy(key = rec.key)
          case Cli.Value     => r.copy(value = rec.value)
          case Cli.Offset    => r.copy(offset = rec.offset)
          case Cli.Headers   => r.copy(headers = rec.headers)
          case Cli.Partition => r.copy(partition = rec.partition)
          case Cli.Timestamp => r.copy(timestamp = rec.timestamp)
        }
      }
    }
    val read: (Vector[RecordDto] => Unit) => Unit = {
      src match {
        case Cli.Std =>
          jsonMapper.readerFor(config.encoding, 128, new ReaderInputStream(stdin, Charset.defaultCharset))
        case Cli.File(name) =>
          fileInputStream = Option(Files.newInputStream(Path.of(name)))
          jsonMapper.readerFor(config.encoding, 128, fileInputStream.get)
        case Cli.Broker(b, t, p, None) => {
          val reader = kafka.mkSrcKafkaReader(b)
          val topicPartitions = p.map(x => List(TopicPartitionDto(t, x))).getOrElse(reader.listTopicPartitions(t))
          reader.read(topicPartitions, _)
        }
        case Cli.Broker(b, t, Some(p), Some(r)) => {
          def adaptFrom(from: Cli.From): Position =
            from match {
              case Cli.First         => First
              case Cli.Beginning     => Beginning
              case Cli.Exact(offset) => Offset(offset)
            }
          def adaptTo(to: Cli.To): Position =
            to match {
              case Cli.Last          => Last
              case Cli.End           => End
              case Cli.Exact(offset) => Offset(offset)
            }
          val (fromPosition, toPosition): (Position, Position) = r match {
            case Cli.Single(x)    => (adaptFrom(x), adaptFrom(x))
            case Cli.Starting(x)  => (adaptFrom(x), Offset(Long.MaxValue))
            case Cli.FromTo(x, y) => (adaptFrom(x), adaptTo(y))
          }
          val reader = kafka.mkSrcKafkaReader(b)
          val range = Set(CopyRange(TopicPartitionDto(t, p), fromPosition, toPosition))
          reader.readTopicPartitions(range, _)
        }
        case Cli.Broker(_, _, None, Some(_)) => ???
      }
    }
    val write: Vector[RecordDto] => Unit =
      dst match {
        case Cli.Std =>
          jsonMapper.writerFor(config.encoding, new WriterOutputStream(stdout, Charset.defaultCharset))
        case Cli.File(name) =>
          fileOutputStream = Option(Files.newOutputStream(Path.of(name)))
          jsonMapper.writerFor(config.encoding, fileOutputStream.get)
        case Cli.Broker(b, _, _, _) => KafkaWriter.mkKafkaWriter(kafka.producerFrom(kafka.dstProducerConfig(b)))
      }
    executeCopy(adapt, read, write)
  }

  def executeCopy(
      adapt: RecordDto => RecordDto,
      read: (Vector[RecordDto] => Unit) => Unit,
      write: Vector[RecordDto] => Unit
  ): Unit = {
    read(xs => write(xs.map(adapt)))
    write(Vector.empty) // final empty write to signal logical completion
  }

}
