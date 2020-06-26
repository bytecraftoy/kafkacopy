// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{InputStream, OutputStream, PrintWriter, Reader}
import java.nio.charset.Charset
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.Properties

import kafkacopy.Cli._
import org.apache.commons.io.input.ReaderInputStream
import org.apache.commons.io.output.WriterOutputStream
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.util.Using

class Demo(private val config: Cli.Config, stdin: Reader, stdout: PrintWriter) {

  import scala.jdk.CollectionConverters._

  private[this] val jsonMapper = new RecordJsonMapper()
  private[this] val log = LoggerFactory.getLogger(classOf[Demo])
  private[this] var outputStream: Option[OutputStream] = None
  private[this] var inputStream: Option[InputStream] = None
  private[this] var admin: Option[Admin] = None
  private[this] var producer: Option[Producer[Array[Byte], Array[Byte]]] = None
  private[this] var consumer: Option[Consumer[Array[Byte], Array[Byte]]] = None

  def close(): Unit = {
    val timeout = Duration.ofSeconds(config.timeoutInSec.get.toLong)
    admin.foreach(_.close(timeout))
    admin = None
    producer.foreach(_.close(timeout))
    producer = None
    consumer.foreach(_.close(timeout))
    consumer = None
    inputStream.foreach(_.close)
    inputStream = None
    outputStream.foreach(_.close)
    outputStream = None
    stdout.flush()
  }

  def listBrokers(stdout: PrintWriter): Unit = brokers.foreach(stdout.println)

  def brokers: List[String] =
    homeFiles()
      .map(_.getFileName)
      .filter(_.getFileName.toString.endsWith(".properties"))
      .map(_.getFileName.toString.dropRight(".properties".length))

  def listTopics(broker: String, stdout: PrintWriter): Unit = {
    listTopics(broker).foreach(stdout.println)
  }

  def listTopics(broker: String): List[String] = {
    mkSingleKafkaReader(broker).listTopics()
  }

  def listPartitions(broker: String, topic: String, stdout: PrintWriter): Unit = {
    listPartitions(broker, topic).foreach { tp =>
      stdout.println(s"${tp.topic}/${tp.partition}")
    }
  }

  def listPartitions(broker: String, topic: String): List[TopicPartitionDto] = {
    mkSingleKafkaReader(broker).listTopicPartitions(topic)
  }

  def listOffsets(broker: String, topic: String, partition: Int, stdout: PrintWriter): Unit = {
    listOffsets(broker, topic, partition).foreach { tp =>
      stdout.println(s"${tp.topic}/${tp.partition}/${tp.beginningOffset}-${tp.endOffset}")
    }
  }

  def listOffsets(broker: String, topic: String, partition: Int): List[OffsetDto] = {
    val reader = mkSingleKafkaReader(broker)
    reader.offsets(reader.listTopicPartitions(topic))
  }

  def createTopic(broker: String, topic: String) = {
    mkAdmin(broker).crateTopic(topic, config.partitions, config.replicationFactor)
  }

  def deleteTopic(broker: String, topic: String) = {
    mkAdmin(broker).deleteTopic(topic)
  }

  def execute(): Unit = {
    try {
      config.cmd match {
        case Some(command) =>
          command match {
            case LsBrokers                           => listBrokers(stdout)
            case LsTopics(broker)                    => listTopics(broker, stdout)
            case LsPartitions(broker, topic)         => listPartitions(broker, topic, stdout)
            case LsOffsets(broker, topic, partition) => listOffsets(broker, topic, partition, stdout)
            case MkTopic(broker, topic)              => createTopic(broker, topic)
            case RmTopic(broker, topic)              => deleteTopic(broker, topic)
            case Copy                                => doCopy()
          }
        case None => ()
      }
    } finally {
      close()
    }
  }

  private def doCopy() = {
    val src = config.srcAddress.get
    val dst = config.dstAddress.get
    val dstPartition = dst match {
      case b: Broker => b.partition
      case _         => None
    }
    val dstTopic = dst match {
      case b: Broker => Option(b.topic)
      case _         => None
    }

    val adapt: RecordDto => RecordDto = { rec =>
      config.dstFields.foldLeft(RecordDto(topic = dstTopic.getOrElse(rec.topic), partition = dstPartition)) { (r, f) =>
        f match {
          case Key       => r.copy(key = rec.key)
          case Value     => r.copy(value = rec.value)
          case Offset    => r.copy(offset = rec.offset)
          case Headers   => r.copy(headers = rec.headers)
          case Partition => r.copy(partition = rec.partition)
          case Timestamp => r.copy(timestamp = rec.timestamp)
        }
      }
    }
    val read: (Vector[RecordDto] => Unit) => Unit = {
      src match {
        case Std => jsonMapper.readerFor(BinaryFormat.Text, 128, new ReaderInputStream(stdin, Charset.defaultCharset))
        case File(name) =>
          inputStream = Option(Files.newInputStream(Path.of(name)))
          jsonMapper.readerFor(BinaryFormat.Text, 128, inputStream.get)
        case Broker(b, t, p) => {
          val reader = mkSrcKafkaReader(b)
          val topicPartitions = p.map(x => List(TopicPartitionDto(t, x))).getOrElse(reader.listTopicPartitions(t))
          reader.read(topicPartitions, _)
        }
      }
    }
    val write: Vector[RecordDto] => Unit =
      dst match {
        case Std => jsonMapper.writerFor(BinaryFormat.Text, new WriterOutputStream(stdout, Charset.defaultCharset))
        case File(name) =>
          outputStream = Option(Files.newOutputStream(Path.of(name)))
          jsonMapper.writerFor(BinaryFormat.Text, outputStream.get)
        case Broker(b, t, p) => KafkaWriter.mkKafkaWriter(producerFrom(dstProducerConfig(b)))
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

  def mkAdmin(broker: String): KafkaAdmin = {
    val admin = adminFrom(adminConfig(broker))
    new KafkaAdmin(admin, Duration.ofSeconds(config.timeoutInSec.get.toLong))
  }

  def producerFrom(cfg: Map[String, String]): Producer[Array[Byte], Array[Byte]] = {
    val producerProps: Properties = new Properties()
    producerProps.putAll(cfg.asJava)
    producer = Option(new KafkaProducer[Array[Byte], Array[Byte]](producerProps))
    producer.get
  }

  def consumerFrom(cfg: Map[String, String]): Consumer[Array[Byte], Array[Byte]] = {
    val props: Properties = new Properties()
    props.putAll(cfg.asJava)
    consumer = Option(new KafkaConsumer[Array[Byte], Array[Byte]](props))
    consumer.get
  }

  def adminFrom(cfg: Map[String, String]): Admin = {
    val props: Properties = new Properties()
    props.putAll(cfg.asJava)
    admin = Option(Admin.create(props))
    admin.get
  }

  def mkSrcKafkaReader(broker: String): KafkaReader =
    new KafkaReader(consumerFrom(srcConsumerConfig(broker)), Duration.ofSeconds(config.timeoutInSec.get.toLong))

  def mkSingleKafkaReader(broker: String): KafkaReader =
    new KafkaReader(consumerFrom(singleConsumerConfig(broker)), Duration.ofSeconds(config.timeoutInSec.get.toLong))

  def adminConfig(broker: String): Map[String, String] =
    defaultAdminConfig ++ loadBrokerConfig(broker) ++ config.singleBrokerProperties.getOrElse(Map.empty)

  def singleConsumerConfig(broker: String): Map[String, String] =
    defaultConsumerConfig ++ loadBrokerConfig(broker) ++ config.singleBrokerProperties.getOrElse(Map.empty)

  def srcConsumerConfig(broker: String): Map[String, String] =
    defaultConsumerConfig ++ loadBrokerConfig(broker) ++ config.srcBrokerProperties.getOrElse(Map.empty)

  def dstProducerConfig(broker: String): Map[String, String] =
    defaultProducerConfig ++ loadBrokerConfig(broker) ++ config.dstBrokerProperties.getOrElse(Map.empty)

  val defaultProducerConfig: Map[String, String] =
    Map(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName
    )

  val defaultConsumerConfig: Map[String, String] =
    Map(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName
    )

  val defaultAdminConfig: Map[String, String] = Map.empty

  def loadBrokerConfig(name: String): Map[String, String] = {
    homeFiles()
      .find(_.getFileName.toString == s"$name.properties")
      .flatMap { file =>
        Using(Files.newInputStream(file)) { in =>
          val p = new java.util.Properties()
          p.load(in)
          Map.empty ++ p.asScala
        }.toOption
      }
      .getOrElse(Map.empty)
  }
  def homeFiles(): List[Path] =
    if (Files.exists(config.homedir)) {
      Files.list(config.homedir).iterator().asScala.toList
    } else List.empty

}
