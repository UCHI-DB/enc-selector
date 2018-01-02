package edu.uchicago.cs.encsel.query.tpch

import org.junit.Assert._
import org.junit.Test

class EncoderTest {

  @Test
  def testEncode: Unit = {
    val result = Encoder.encode(Array(1, 3, 20, 10, 31, 1, 7, 56, 8, 22, 18, 36, 44, 9), 6)

    assertEquals(12, result.array().length)

    assertEquals(0x5f2940c1, result.getInt())
    assertEquals(0x2588e070, result.getInt())
    assertEquals(0x26c91, result.getInt())
  }
}
