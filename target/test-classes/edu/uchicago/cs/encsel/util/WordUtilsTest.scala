package edu.uchicago.cs.encsel.util

import org.junit.Test
import org.junit.Assert._
import org.nd4j.linalg.factory.Nd4j

class WordUtilsTest {

  @Test
  def testLevDist2: Unit = {
    val a = "actual"
    val b = "actua"

    println(WordUtils.levDistance2(a, b))
  }
  

}