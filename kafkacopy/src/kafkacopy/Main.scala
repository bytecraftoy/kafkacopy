// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.io.{PrintWriter, Reader}

object Main {

  def main(args: Array[String]): Unit = {
    val reader = System.console().reader()
    val writer = System.console().writer()
    run(args, reader, writer)
  }

  def run(args: Array[String], stdin: Reader, stdout: PrintWriter): Unit = {
    val conf = Cli.cli(args)
    new KafkaCopy(conf.get, stdin, stdout).execute()
  }

}
