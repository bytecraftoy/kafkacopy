// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import kafkacopy.BinaryEncoding.BinaryEncoding

class RecordJsonMapper(private val mapper: ObjectMapper) {
  def this() = {
    this(
      new ObjectMapper(
        new JsonFactory()
          .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
          .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false)
      ).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.CLOSE_CLOSEABLE, false)
    )
  }

  private val reader = mapper.readerFor(classOf[ObjectNode])
  private val writer = mapper.writerFor(classOf[ObjectNode])

  def reader(in: InputStream): Iterator[ObjectNode] = {
    import scala.jdk.CollectionConverters._
    reader.readValues[ObjectNode](in).asScala
  }

  def writer(out: OutputStream): ObjectNode => Unit = { r =>
    writer.writeValue(out, r)
    out.write('\n')
  }

  def to(f: JsonNode => Array[Byte], o: ObjectNode): RecordDto = {
    def nodeOpt(node: JsonNode): Option[JsonNode] =
      Option(node).flatMap(x =>
        if (x.isNull) None
        else Option(x)
      )

    RecordDto(
      Option(o.get("topic").textValue()).get,
      nodeOpt(o.get("partition")).map(_.intValue()),
      nodeOpt(o.get("offset")).map(_.longValue()),
      nodeOpt(o.get("headers"))
        .map { hs =>
          val arr: ArrayNode = hs.asInstanceOf[ArrayNode]
          import scala.jdk.CollectionConverters._
          arr.iterator().asScala.toList.map { h =>
            HeaderDto(Option(h.get("key").textValue()).get, nodeOpt(h.get("value")).map(f))
          }
        }
        .getOrElse(List.empty),
      nodeOpt(o.get("key")).map(f),
      nodeOpt(o.get("value")).map(f),
      nodeOpt(o.get("timestamp")).map(_.longValue())
    )
  }

  def from(f: Array[Byte] => JsonNode, r: RecordDto): ObjectNode = {
    val o = JsonNodeFactory.instance.objectNode()
    o.put("topic", r.topic)
    r.partition.map(java.lang.Integer.valueOf).foreach {
      o.put("partition", _)
    }
    r.offset.map(java.lang.Long.valueOf).foreach {
      o.put("offset", _)
    }
    if (r.headers.nonEmpty) {
      val headers = JsonNodeFactory.instance.arrayNode()
      r.headers.foreach { rh =>
        val header = JsonNodeFactory.instance.objectNode()
        header.put("key", rh.key)
        header.set("value", rh.value.map(f).orNull)
        headers.add(header)
      }
      o.set("headers", headers)
    }
    o.set("key", r.key.map(f).orNull)
    o.set("value", r.value.map(f).orNull)
    r.timestamp.map(java.lang.Long.valueOf).foreach {
      o.put("timestamp", _)
    }
    o
  }

  private val toText: Array[Byte] => JsonNode = xs => TextNode.valueOf(new java.lang.String(xs, UTF_8))
  private val toJson: Array[Byte] => JsonNode = xs => mapper.readValue(xs, classOf[JsonNode])
  private val toBase64: Array[Byte] => JsonNode = xs => TextNode.valueOf(java.util.Base64.getEncoder.encodeToString(xs))
  private val fromText: JsonNode => Array[Byte] = _.textValue().getBytes(UTF_8)
  private val fromJson: JsonNode => Array[Byte] = mapper.writeValueAsBytes(_)
  private val fromBase64: JsonNode => Array[Byte] = node => java.util.Base64.getDecoder.decode(node.textValue())

  def readerFor(encoding: BinaryEncoding, batchSize: Int, in: InputStream): (Vector[RecordDto] => Unit) => Unit = {
    val fmt = encoding match {
      case BinaryEncoding.Text   => fromText
      case BinaryEncoding.Json   => fromJson
      case BinaryEncoding.Base64 => fromBase64
    }
    reader(in)
      .map(to(fmt, _))
      .sliding(batchSize, batchSize)
      .map(_.toVector)
      .foreach(_)

  }
  def writerFor(format: BinaryEncoding, out: OutputStream): Vector[RecordDto] => Unit = {
    val fmt = format match {
      case BinaryEncoding.Text   => toText
      case BinaryEncoding.Json   => toJson
      case BinaryEncoding.Base64 => toBase64
    }
    val write: RecordDto => Unit = writer(out).compose(from(fmt, _))
    xs =>
      if (xs.isEmpty) out.flush()
      else xs.foreach(write)
  }

}
