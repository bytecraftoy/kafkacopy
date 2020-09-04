// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

object BinaryEncoding extends Enumeration {
  type BinaryEncoding = Value
  val Text, Json, Base64 = Value
}
