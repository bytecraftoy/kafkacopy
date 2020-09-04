// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{File, PrintWriter, StringReader, StringWriter}
import java.nio.file.Files

import kafka.EmbeddedKafka
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class KafkaCopySpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  private var stdin = new StringReader("")
  private var stdoutSw = new StringWriter()
  private var stdout = new PrintWriter(stdoutSw)
  private var embeddedKafka: Option[EmbeddedKafka] = None

  private def bootstrapServers: String = embeddedKafka.get.getBrokersAsString

  override def beforeAll(): Unit = {
    embeddedKafka = Option(new EmbeddedKafka(1, false, 1))
    embeddedKafka.foreach(_.start())
  }

  override def afterAll(): Unit = embeddedKafka.foreach(_.close())

  before {
    stdin = new StringReader("")
    stdoutSw = new StringWriter()
    stdout = new PrintWriter(stdoutSw)
  }

  describe("kafkacopy") {
    val key = "hello"
    val value = "world"

    it("should create a topics") {
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "2", "a/foo", "-t", "1")
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "3", "b/bar", "-t", "1")
    }

    it("should list topics") {
      run("ls", "-p", s"bootstrap.servers=$bootstrapServers", "a")
      val actual = stdoutSw.toString
      actual shouldBe "bar\nfoo\n"
    }

    it("should read from stdin and copy to target topic") {
      stdin = new StringReader(s"""{"topic":"a","key":"$key","value":"$value"}""")
      run("copy", "@-", "-d", s"bootstrap.servers=$bootstrapServers", "a/foo", "-t", "1")
    }

    it("should copy from source topic to target topic") {
      run("copy",
          "-s",
          s"bootstrap.servers=$bootstrapServers",
          "a/foo",
          "-d",
          s"bootstrap.servers=$bootstrapServers",
          "b/bar",
          "-t",
          "1")
    }

    it("should copy topic to stdout") {
      run("copy", "-s", s"bootstrap.servers=$bootstrapServers", "a/foo", "@-", "-t", "1")
      val expected =
        s"""{"topic":"foo","key":"$key","value":"$value","timestamp":1}
           |""".stripMargin
      val actual = stdoutSw.toString
        .replaceAll(""""timestamp":\d+""", """"timestamp":1""")
      actual shouldBe expected
    }
  }

  describe("asdfasdfasdf") {
    it("should create a topics") {
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "2", "a/x", "-t", "1")
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "3", "b/y", "-t", "1")
    }

    it("should read from stdin and copy to target topic") {
      stdin = new StringReader(s"""{"topic":"x","key":"1","value":"a"}
           |{"topic":"x","key":"2","value":"b"}
           |{"topic":"x","key":"3","value":"c"}
           |{"topic":"x","key":"4","value":"d"}
           |""".stripMargin)
      run("copy", "@-", "-d", s"bootstrap.servers=$bootstrapServers", "a/x/1", "-t", "1")
    }

    it("should copy single message to target partition") {
      run("copy",
          "-s",
          s"bootstrap.servers=$bootstrapServers",
          "a/x/1/2",
          "-d",
          s"bootstrap.servers=$bootstrapServers",
          "b/y/2",
          "-t",
          "1")
    }

    it("should copy topic to stdout") {
      run("copy", "-s", s"bootstrap.servers=$bootstrapServers", "a/y", "@-", "-t", "1")
      val expected =
        s"""{"topic":"y","key":"3","value":"c","timestamp":1}
           |""".stripMargin
      val actual = stdoutSw.toString
        .replaceAll(""""timestamp":\d+""", """"timestamp":1""")
      actual shouldBe expected
    }
  }

  private def run(args: String*): Unit = {
    Main.run(
      args.toArray,
      stdin,
      stdout
    )
  }

}
