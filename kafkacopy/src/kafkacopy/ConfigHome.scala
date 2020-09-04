// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

import java.nio.file.{Files, Path}

import scala.util.Using

class ConfigHome(private val config: Cli.Config) {

  import scala.jdk.CollectionConverters._

  def loadBrokerConfig(name: String): Map[String, String] = {
    homeFiles()
      .find(_.getFileName.toString == s"$name.properties")
      .flatMap { file =>
        Using(Files.newInputStream(file)) { in =>
          val p = new java.util.Properties()
          p.load(in)
          Map.empty ++ p.asScala
        }.toOption
      }
      .getOrElse(Map.empty)
  }
  def homeFiles(): List[Path] =
    if (Files.exists(config.homedir)) {
      Files.list(config.homedir).iterator().asScala.toList
    } else List.empty

}
