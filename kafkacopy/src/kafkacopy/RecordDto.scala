// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

case class RecordDto(
    topic: String,
    partition: Option[Int] = None,
    offset: Option[Long] = None,
    headers: List[HeaderDto] = List.empty,
    key: Option[Array[Byte]] = None,
    value: Option[Array[Byte]] = None,
    timestamp: Option[Long] = None
)
