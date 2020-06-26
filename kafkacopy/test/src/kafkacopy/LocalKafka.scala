// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

object LocalKafka {
  def main(args: Array[String]): Unit = {
    EmbeddedKafka.withEmbeddedKafka(1, false, 1) { brokers =>
      println()
      import scala.io.StdIn.readLine
      readLine(s"""Local kafka running at: ${brokers.bootstrapServers}
           |ctrl-c to terminate""".stripMargin)
      println()
      println("Terminating...")
    }
  }
}
