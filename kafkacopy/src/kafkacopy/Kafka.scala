// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.PrintWriter
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

class Kafka(private val config: Cli.Config, private val cfgHome: ConfigHome) {

  import scala.jdk.CollectionConverters._

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
  }

  def listBrokers(stdout: PrintWriter): Unit = brokers.foreach(stdout.println)

  def brokers: List[String] =
    cfgHome
      .homeFiles()
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
    defaultAdminConfig ++ cfgHome.loadBrokerConfig(broker) ++ config.singleBrokerProperties.getOrElse(Map.empty)

  def singleConsumerConfig(broker: String): Map[String, String] =
    defaultConsumerConfig ++ cfgHome.loadBrokerConfig(broker) ++ config.singleBrokerProperties.getOrElse(Map.empty)

  def srcConsumerConfig(broker: String): Map[String, String] =
    defaultConsumerConfig ++ cfgHome.loadBrokerConfig(broker) ++ config.srcBrokerProperties.getOrElse(Map.empty)

  def dstProducerConfig(broker: String): Map[String, String] =
    defaultProducerConfig ++ cfgHome.loadBrokerConfig(broker) ++ config.dstBrokerProperties.getOrElse(Map.empty)

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

}
