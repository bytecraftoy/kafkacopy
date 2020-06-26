// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

package kafkacopy

case class TopicPartitionDto(topic: String, partition: Int) extends Ordered[TopicPartitionDto] {

  override def compare(that: TopicPartitionDto): Int =
    TopicPartitionDto.ord.compare(this, that)

}
object TopicPartitionDto {
  private val ord = Ordering.by(unapply)
}
