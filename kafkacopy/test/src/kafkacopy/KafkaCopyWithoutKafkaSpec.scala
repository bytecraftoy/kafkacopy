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

class KafkaCopyWithoutKafkaSpec extends AnyFunSpec with Matchers {

  private var stdin = new StringReader("")
  private var stdoutSw = new StringWriter()
  private var stdout = new PrintWriter(stdoutSw)

  describe("kafkacopy") {
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

}
