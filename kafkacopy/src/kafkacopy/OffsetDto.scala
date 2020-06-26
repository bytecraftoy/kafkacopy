// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

case class OffsetDto(val topic: String, val partition: Int, val beginningOffset: Long, val endOffset: Long)
    extends Ordered[OffsetDto] {

  override def compare(that: OffsetDto): Int =
    OffsetDto.ord.compare(this, that)

}
object OffsetDto {
  private val ord = Ordering.by(unapply)
}
