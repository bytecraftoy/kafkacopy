// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

sealed trait Position
case object Beginning extends Position
case object First extends Position
case object Last extends Position
case object End extends Position
case class Offset(offset: Long) extends Position
