// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RecordJsonMapperSpec extends AnyFunSpec with Matchers {

  describe("Json read") {
    val mapper = new RecordJsonMapper()
    val strf: JsonNode => Array[Byte] = _.textValue().getBytes(UTF_8)
    def read(json: String): List[RecordDto] = {
      mapper
        .reader(new ByteArrayInputStream(json.getBytes(UTF_8)))
        .map(mapper.to(strf, _))
        .toList

    }
    it("ignore garbage") {
      read("""{"topic":"a", "foo":1}""") should be(List(RecordDto("a")))
    }
    it("parse topic") {
      read("""{"topic":"a"}""") should be(List(RecordDto("a")))
      assertThrows[Exception] {
        read("""{"topic": null}""")
      }
      assertThrows[Exception] {
        read("""{}""")
      }
    }
    it("parse partition") {
      read("""{"topic":"a","partition": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","partition": 1}""") should be(List(RecordDto(topic = "a", partition = Option(1))))
    }
    it("parse offset") {
      read("""{"topic":"a","offset": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","offset": 1}""") should be(List(RecordDto(topic = "a", offset = Option(1))))
    }
    it("parse timestamp") {
      read("""{"topic":"a","timestamp": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","timestamp": 1}""") should be(List(RecordDto(topic = "a", timestamp = Option(1))))
    }
    it("parse key") {
      read("""{"topic":"a","key": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","key": "k"}""").head.key.get should equal("k".getBytes(UTF_8))
    }
    it("parse value") {
      read("""{"topic":"a","value": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","value": "v"}""").head.value.get should equal("v".getBytes(UTF_8))
    }
    it("parse headers") {
      read("""{"topic":"a","headers": null}""") should be(List(RecordDto(topic = "a")))
      read("""{"topic":"a","headers": []}""") should be(List(RecordDto(topic = "a")))
      assertThrows[Exception] {
        read("""{"topic":"a","headers": [{"key": null}]}""")
      };
      {
        val actual = read("""{"topic":"a","headers": [{"key": "h"}]}""")
        val expected = List(RecordDto(topic = "a", headers = List(HeaderDto("h", None))))
        actual should be(expected)
      };
      {
        val actual = read("""{"topic":"a","headers": [{"key": "h", "value": null}]}""")
        val expected = List(RecordDto(topic = "a", headers = List(HeaderDto("h", None))))
        actual should be(expected)
      };
      {
        val actual = read("""{"topic":"a","headers": [{"key": "h", "value": "i"}]}""")
        actual.head.headers.head.value.get should equal("i".getBytes(UTF_8))

      };
    }
    it("parse multiple records") {
      read("""{"topic":"a"}
             |{"topic":"b"}""".stripMargin) should be(List(RecordDto(topic = "a"), RecordDto(topic = "b")))
    }
  }
  describe("Json write") {
    val mapper = new RecordJsonMapper()
    val strf: Array[Byte] => JsonNode = xs => {
      JsonNodeFactory.instance.textNode(new java.lang.String(xs, UTF_8))
    }
    def write(rs: List[RecordDto]): String = {
      val out = new ByteArrayOutputStream()
      val write = mapper.writer(out).compose(mapper.from(strf, _))
      rs.foreach { write }
      new java.lang.String(out.toByteArray, UTF_8)
    }
    it("write one") {
      write(List(RecordDto("a"))) should be("""{"topic":"a","key":null,"value":null}
          |""".stripMargin)
    }
    it("write two") {
      write(List(RecordDto("a"), RecordDto("b"))) should be("""{"topic":"a","key":null,"value":null}
          |{"topic":"b","key":null,"value":null}
          |""".stripMargin)
    }
    it("write all fields") {
      val rec =
        RecordDto(
          "a",
          Option(1),
          Option(2),
          List(HeaderDto("h1", None), HeaderDto("h2", Option("v2".getBytes(UTF_8)))),
          Option("k".getBytes(UTF_8)),
          Option("v".getBytes(UTF_8)),
          Option(3)
        )
      write(List(rec)) should be(
        """{"topic":"a","partition":1,"offset":2,"headers":[{"key":"h1","value":null},{"key":"h2","value":"v2"}],"key":"k","value":"v","timestamp":3}
          |""".stripMargin)
    }
  }
}
