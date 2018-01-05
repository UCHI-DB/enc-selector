package edu.uchicago.cs.encsel.query.tpch

import edu.uchicago.cs.encsel.query.offheap.EqualScalar
import org.junit.Assert._
import org.junit.Test

class EqualScalarTest {

  @Test
  def testPredicate: Unit = {

    val entryWidth = 6
    val pred = new EqualScalar(35, entryWidth)
    val data = Array(3, 1, 5, 2, 35, 0, 0, 1, 7, 35, 0, 0, 1, 2, 3, 35, 0, 0, 35, 1)
    val encoded = Encoder.encode(data, entryWidth)

    val result = pred.execute(encoded, 0, data.length)

    assertEquals(0x10.toByte, result.get(0))
    assertEquals(0x82.toByte, result.get(1))
    assertEquals(0x4.toByte, result.get(2))
  }
}
