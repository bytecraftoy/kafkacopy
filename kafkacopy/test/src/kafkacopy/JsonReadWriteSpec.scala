// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class JsonReadWriteSpec extends AnyFunSpec with Matchers {

  describe("Json read write") {
    val mapper = new RecordJsonMapper()
    it("stream batching") {
      val out = new ByteArrayOutputStream()
      val write: Vector[RecordDto] => Unit = mapper.writerFor(BinaryFormat.Text, out)
      write(Vector("a", "b", "c").map(RecordDto(_)))
      write(Vector("1", "2", "3").map(RecordDto(_)))
      write(Vector("x", "y", "z").map(RecordDto(_)))
      val read: (Vector[RecordDto] => Unit) => Unit =
        mapper.readerFor(BinaryFormat.Text, 2, new ByteArrayInputStream(out.toByteArray))
      var actual = Vector.empty[Vector[String]]
      read { actual :+= _.map(_.topic) }
      val expected = Vector(Vector("a", "b"), Vector("c", "1"), Vector("2", "3"), Vector("x", "y"), Vector("z"))
      actual should be(expected)
    }
    it("text encoding") {
      val out = new ByteArrayOutputStream()
      val write: Vector[RecordDto] => Unit = mapper.writerFor(BinaryFormat.Text, out)
      val expected =
        """{"topic":"a","headers":[{"key":"b","value":"h"}],"key":"k","value":"v"}
          |""".stripMargin
      val read: (Vector[RecordDto] => Unit) => Unit =
        mapper.readerFor(BinaryFormat.Text, 2, new ByteArrayInputStream(expected.getBytes(UTF_8)))
      read(write)
      val actual = new java.lang.String(out.toByteArray)
      actual should be(expected)
    }
    it("json encoding") {
      val out = new ByteArrayOutputStream()
      val write: Vector[RecordDto] => Unit = mapper.writerFor(BinaryFormat.Json, out)
      val expected =
        """{"topic":"a","headers":[{"key":"b","value":{"h":1}}],"key":{"k":1},"value":[{"v":1}]}
          |""".stripMargin
      val read: (Vector[RecordDto] => Unit) => Unit =
        mapper.readerFor(BinaryFormat.Json, 2, new ByteArrayInputStream(expected.getBytes(UTF_8)))
      read(write)
      val actual = new java.lang.String(out.toByteArray)
      actual should be(expected)
    }
    it("base64 encoding") {
      val out = new ByteArrayOutputStream()
      val write: Vector[RecordDto] => Unit = mapper.writerFor(BinaryFormat.Base64, out)
      val expected =
        """{"topic":"a","headers":[{"key":"b","value":"aA=="}],"key":"aw==","value":"dg=="}
          |""".stripMargin
      val read: (Vector[RecordDto] => Unit) => Unit =
        mapper.readerFor(BinaryFormat.Base64, 2, new ByteArrayInputStream(expected.getBytes(UTF_8)))
      read(write)
      val actual = new java.lang.String(out.toByteArray)
      actual should be(expected)
    }
  }
}
