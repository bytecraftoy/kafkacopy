// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.nio.file.Paths

import kafkacopy.Cli._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CliSpec extends AnyFunSpec with Matchers {

  describe("Cli parsing") {
    it("print help") {
      Cli
        .cli(Array(), showUsage = true)
    }
    it("should parse home dir") {
      val expected = Some(Paths.get("test/resources/samplehome1"))
      Cli
        .cli(Array("ls", "--homedir", "test/resources/samplehome1/"))
        .map(_.homedir) should be(expected)
    }
    it("should parse ls") {
      Cli.cli(Array("ls")).flatMap(_.cmd) should be(Some(LsBrokers))
    }
    it("should parse ls broker") {
      Cli.cli(Array("ls", "broker")).flatMap(_.cmd) should be(Some(LsTopics("broker")))
    }
    it("should parse ls options") {
      val conf = Cli.cli(Array("ls", "-p", "k1=v1,k2=v2", "broker"))
      conf.flatMap(_.cmd) should be(Some(LsTopics("broker")))
      conf.flatMap(_.singleBrokerProperties) should be(Some(Map("k1" -> "v1", "k2" -> "v2")))
    }
    it("should parse ls broker/topic/partition") {
      Cli.cli(Array("ls", "broker/topic/12")).flatMap(_.cmd) should be(Some(LsOffsets("broker", "topic", 12)))
    }
    it("should parse rmtopic broker/topic") {
      Cli.cli(Array("rmtopic", "broker/topic")).flatMap(_.cmd) should be(Some(RmTopic("broker", "topic")))
    }
    it("should parse rmtopic broker/topic with options") {
      val conf = Cli.cli(Array("rmtopic", "-p", "k1=v1,k2=v2", "broker/topic"))
      conf.flatMap(_.cmd) should be(Some(RmTopic("broker", "topic")))
      conf.flatMap(_.singleBrokerProperties) should be(Some(Map("k1" -> "v1", "k2" -> "v2")))
    }
    it("should parse mktopic broker/topic") {
      Cli.cli(Array("mktopic", "broker/topic")).flatMap(_.cmd) should be(Some(MkTopic("broker", "topic")))
    }
    it("should parse mktopic broker/topic with options") {
      val conf =
        Cli.cli(Array("mktopic", "-p", "k1=v1,k2=v2", "--replication-factor", "1", "--partitions", "2", "broker/topic"))
      conf.flatMap(_.cmd) should be(Some(MkTopic("broker", "topic")))
      conf.flatMap(_.singleBrokerProperties) should be(Some(Map("k1" -> "v1", "k2" -> "v2")))
      conf.flatMap(_.replicationFactor) should be(Some(1))
      conf.flatMap(_.partitions) should be(Some(2))
    }
    it("should parse mktopic broker/topic with topic options") {
      val conf = Cli.cli(Array("mktopic", "-c", "k1=v1,k2=v2", "broker/topic"))
      conf.flatMap(_.cmd) should be(Some(MkTopic("broker", "topic")))
      conf.flatMap(_.singleTopicProperties) should be(Some(Map("k1" -> "v1", "k2" -> "v2")))
    }
    it("should parse copy brokers") {
      val conf = Cli.cli(Array("copy", "broker1/topic1", "broker2/topic2/42"))
      conf.flatMap(_.cmd) should be(Some(Copy))
      conf.flatMap(_.srcAddress) should be(Some(Broker("broker1", "topic1")))
      conf.flatMap(_.dstAddress) should be(Some(Broker("broker2", "topic2", Some(42))))
    }
    it("should parse copy file") {
      val conf = Cli.cli(Array("copy", "@myfile", "broker2/topic2/42"))
      conf.flatMap(_.cmd) should be(Some(Copy))
      conf.flatMap(_.srcAddress) should be(Some(File("myfile")))
      conf.flatMap(_.dstAddress) should be(Some(Broker("broker2", "topic2", Some(42))))
    }
    it("should parse copy stdin") {
      val conf = Cli.cli(Array("copy", "@-", "broker2/topic2/42"))
      conf.flatMap(_.cmd) should be(Some(Copy))
      conf.flatMap(_.srcAddress) should be(Some(Std))
      conf.flatMap(_.dstAddress) should be(Some(Broker("broker2", "topic2", Some(42))))
    }
    it("should parse copy with configs") {
      val conf = Cli.cli(
        Array("copy", "-s", "a=b", "broker1/topic1", "-d", "c=d", "-f", "value", "broker2/topic2/42", "-t", "1"))
      conf.flatMap(_.timeoutInSec) should be(Some(1))
      conf.flatMap(_.cmd) should be(Some(Copy))
      conf.flatMap(_.srcAddress) should be(Some(Broker("broker1", "topic1")))
      conf.flatMap(_.dstAddress) should be(Some(Broker("broker2", "topic2", Some(42))))
      conf.flatMap(_.srcBrokerProperties) should be(Some(Map("a" -> "b")))
      conf.flatMap(_.dstBrokerProperties) should be(Some(Map("c" -> "d")))
      conf.map(_.dstFields) should be(Some(Set(Value)))
    }
  }
}
