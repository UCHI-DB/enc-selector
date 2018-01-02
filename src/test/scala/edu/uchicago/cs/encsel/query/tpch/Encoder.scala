package edu.uchicago.cs.encsel.query.tpch

import java.nio.ByteBuffer

object Encoder {

  def encode(input: Array[Int], entryWidth: Int): ByteBuffer = {
    val byteSize = Math.ceil((entryWidth * input.size).toDouble / 32).toInt * 4

    val result = ByteBuffer.allocate(byteSize)

    var buffer = 0
    var offset = 0

    for (i <- input.indices) {
      buffer = buffer | (input(i) << offset)
      offset = offset + entryWidth
      if (offset >= 32) {
        offset = offset - 32
        result.putInt(buffer)
        buffer = input(i) >> (entryWidth - offset)
      }
    }
    if (offset != 0)
      result.putInt(buffer)
    result.flip()
    return result
  }
}
