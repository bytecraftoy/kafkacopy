// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{File, PrintWriter, StringReader, StringWriter}
import java.nio.file.Files

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class KafkaCopySpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  describe("kafkacopy") {
    val key = "hello"
    val value = "world"

    it("should create a topics") {
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "2", "a/foo", "-t", "1")
      run("mktopic", "-p", s"bootstrap.servers=$bootstrapServers", "--partitions", "3", "b/bar", "-t", "1")
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
      val expected = s"""{"topic":"foo","key":"$key","value":"$value","timestamp":1}
                        |""".stripMargin
      val actual = stdoutSw.toString
        .replaceAll(""""timestamp":\d+""", """"timestamp":1""")
      actual shouldBe expected
    }

    it("copy files") {
      val srcFile = File.createTempFile("copy", ".tmp")
      srcFile.deleteOnExit()
      val dstFile = File.createTempFile("copy", ".tmp")
      dstFile.deleteOnExit()
      val input =
        """{"topic":"a","key":"k","value":"v"}
          |""".stripMargin
      Files.writeString(srcFile.toPath, input)

      run("copy", s"@${srcFile.getAbsolutePath}", "-f", "value", s"@${dstFile.getAbsolutePath}")

      val actual = Files.readString(dstFile.toPath)
      val expected = """{"topic":"a","key":null,"value":"v"}
                       |""".stripMargin
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

  before {
    stdin = new StringReader("")
    stdoutSw = new StringWriter()
    stdout = new PrintWriter(stdoutSw)
  }

  private var stdin = new StringReader("")
  private var stdoutSw = new StringWriter()
  private var stdout = new PrintWriter(stdoutSw)
  private var embeddedKafka: Option[EmbeddedKafka] = None

  private def bootstrapServers: String = embeddedKafka.get.getBrokersAsString.bootstrapServers

  override def beforeAll(): Unit = {
    embeddedKafka = Option(new EmbeddedKafka(1, false, 1))
    embeddedKafka.foreach(_.start())
  }

  override def afterAll(): Unit = embeddedKafka.foreach(_.close())

}
