// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.nio.file.{Path, Paths}

import scala.util.matching.Regex

object Cli {
  // config precedence: cli > properties > env > configfile > defaults

  def propertyOrElseEnv(name: String): Option[String] = {
    Option(System.getProperty(name)).orElse(Option(System.getenv(name)))
  }

  private val HOME_NAME = "KAFKACOPY_HOME"

  def defaultHomeDir(): Path =
    Paths.get(propertyOrElseEnv(HOME_NAME).getOrElse(System.getProperty("user.home") + "/.kafkacopy"))

  private val FileRe: Regex = "@(.*)".r
  private val StdRe: Regex = "@-".r
  private val BrokerRe: Regex = "([^/]+)".r
  private val BrokerTopicRe = "([^/]+)/([^/]+)+".r
  private val BrokerTopicPartitionRe = "([^/]+)/([^/]+)/([\\d]+)+".r

  abstract sealed trait Cmd
  case object LsBrokers extends Cmd
  case class LsTopics(broker: String) extends Cmd
  case class LsPartitions(broker: String, topic: String) extends Cmd
  case class LsOffsets(broker: String, topic: String, partition: Int) extends Cmd
  case class MkTopic(broker: String, topic: String) extends Cmd
  case class RmTopic(broker: String, topic: String) extends Cmd
  case object Copy extends Cmd

  sealed abstract trait Address
  case class Broker(broker: String, topic: String, partition: Option[Int] = None) extends Address
  case class File(name: String) extends Address
  case object Std extends Address

  sealed abstract trait Field
  case object Key extends Field
  case object Value extends Field
  case object Offset extends Field
  case object Headers extends Field
  case object Partition extends Field
  case object Timestamp extends Field
  object Field {
    val all = Set(Key, Value, Offset, Headers, Partition, Timestamp)
    def parse(s: Seq[String]): Set[Field] =
      s.map(_.toLowerCase match {
          case "key"       => Key
          case "value"     => Value
          case "offset"    => Offset
          case "headers"   => Headers
          case "partition" => Partition
          case "timestamp" => Timestamp
        })
        .toSet
  }

  case class Config(
      homedir: Path = defaultHomeDir(),
      cmd: Option[Cmd] = None,
      singleBrokerProperties: Option[Map[String, String]] = None,
      singleTopicProperties: Option[Map[String, String]] = None,
      srcBrokerProperties: Option[Map[String, String]] = None,
      dstBrokerProperties: Option[Map[String, String]] = None,
      timeoutInSec: Option[Int] = None,
      srcAddress: Option[Address] = None,
      dstAddress: Option[Address] = None,
      replicationFactor: Option[Int] = None,
      partitions: Option[Int] = None,
      dstFields: Set[Field] = Set(Key, Value, Headers, Timestamp)
  )

  def cli(args: Array[String], showUsage: Boolean = false): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("kafkacopy") {
      head("kafkacopy", "x.y.z")

      opt[String]("homedir")
        .action((home, c) => c.copy(homedir = Paths.get(home)))
        .text(
          s"kafkacopy home dir. defaults to $$HOME/.kafkacopy/, '$HOME_NAME' java property or environment variable are also respected."
        )
      opt[Int]('t', "timeout")
        .text("timeout in seconds for kafka operations such as polling. default is 10.")
        .action {
          case (timeout, c) => c.copy(timeoutInSec = Some(timeout))
        }
        .withFallback { () =>
          10
        }
      cmd("ls")
        .action((_, c) => c.copy(cmd = Some(LsBrokers)))
        .text("list brokers (pre-configured), topics of broker, partitions of topic or topic offsets.")
        .children(
          opt[Map[String, String]]('p', "properties")
            .valueName("k1=v1,k2=v2...")
            .text("kafka properties")
            .action {
              case (o, c) => c.copy(singleBrokerProperties = Some(o))
            },
          arg[String]("broker[/topic[/partition]]")
            .action(
              (s, c) =>
                s match {
                  case BrokerRe(b)                     => c.copy(cmd = Some(LsTopics(b)))
                  case BrokerTopicRe(b, t)             => c.copy(cmd = Some(LsPartitions(b, t)))
                  case BrokerTopicPartitionRe(b, t, p) => c.copy(cmd = Some(LsOffsets(b, t, p.toInt)))
                }
            )
            .optional()
        )

      cmd("mktopic")
        .text("create topic")
        .children(
          opt[Map[String, String]]('p', "properties")
            .valueName("k1=v1,k2=v2...")
            .text("kafka properties")
            .action {
              case (o, c) => c.copy(singleBrokerProperties = Some(o))
            },
          opt[Map[String, String]]('c', "config")
            .valueName("k1=v1,k2=v2...")
            .text("topic configuration")
            .action {
              case (o, c) => c.copy(singleTopicProperties = Some(o))
            },
          opt[Int]("partitions")
            .text("number of partitions")
            .action {
              case (o, c) => c.copy(partitions = Some(o))
            },
          opt[Int]("replication-factor")
            .text("replication factor")
            .action {
              case (o, c) => c.copy(replicationFactor = Some(o))
            },
          arg[String]("broker/topic")
            .action(
              (s, c) =>
                s match {
                  case BrokerTopicRe(b, t) => c.copy(cmd = Some(MkTopic(b, t)))
                }
            )
            .text("topic name to create in broker")
        )
      cmd("rmtopic")
        .text("delete topic")
        .children(
          opt[Map[String, String]]('p', "properties")
            .valueName("k1=v1,k2=v2...")
            .text("kafka properties")
            .action {
              case (o, c) => c.copy(singleBrokerProperties = Some(o))
            },
          arg[String]("broker/topic")
            .action(
              (s, c) =>
                s match {
                  case BrokerTopicRe(b, t) => c.copy(cmd = Some(RmTopic(b, t)))
                }
            )
            .text("topic name to delete from broker")
        )

      cmd("copy")
        .action((_, c) => c.copy(cmd = Some(Copy)))
        .text("copy from <src> to <dst>")
        .children(
          opt[Map[String, String]]('s', "src-config")
            .valueName("k1=v1,k2=v2...")
            .text("source kafka configuration")
            .action {
              case (o, c) => c.copy(srcBrokerProperties = Some(o))
            },
          arg[String]("<src>")
            .required()
            .text("source")
            .action(
              (s, c) =>
                s match {
                  case StdRe()                         => c.copy(srcAddress = Some(Std))
                  case FileRe(name)                    => c.copy(srcAddress = Some(File(name)))
                  case BrokerTopicRe(b, t)             => c.copy(srcAddress = Some(Broker(b, t)))
                  case BrokerTopicPartitionRe(b, t, p) => c.copy(srcAddress = Some(Broker(b, t, Some(p.toInt))))
                }
            ),
          opt[Map[String, String]]('d', "dst-config")
            .valueName("k1=v1,k2=v2...")
            .text("dst kafka configuration")
            .action {
              case (o, c) => c.copy(dstBrokerProperties = Some(o))
            },
          opt[Seq[String]]('f', "fields")
            .text(
              "record fields to copy. valid values: key, value, headers and timestamp. by default all fields are copied."
            )
            .action {
              case (o, c) => c.copy(dstFields = Field.parse(o))
            },
          arg[String]("<dst>")
            .required()
            .text("target")
            .action(
              (s, c) =>
                s match {
                  case StdRe()                         => c.copy(dstAddress = Some(Std))
                  case FileRe(name)                    => c.copy(dstAddress = Some(File(name)))
                  case BrokerTopicRe(b, t)             => c.copy(dstAddress = Some(Broker(b, t)))
                  case BrokerTopicPartitionRe(b, t, p) => c.copy(dstAddress = Some(Broker(b, t, Some(p.toInt))))
                }
            )
        )
      note(
        "<src> and <dst> can be local files (@filename), stdin/stdout (@-), or kafka (broker/topic[/partition/[RANGE]]).\n" +
          "  RANGE := [ offset | FROM-[TO] ]\n" +
          "  FROM := [ offset | BEGINNING | FIRST ]\n" +
          "  TO := [ offset | END | LAST ]\n" +
          "  RANGE is inclusive and valid only for <src>. default RANGE is FIRST-LAST. if TO is omitted then input is read until terminated.\n"
      )

      help("help").text("prints this usage text")

    }
    if (showUsage || args.isEmpty) parser.showUsage()
    parser.parse(args, Config())
  }

}
