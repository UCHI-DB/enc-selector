package edu.uchicago.cs.encsel.dataset.feature.classify

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.classify.BlockSimilarWords.msgSize
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class SimilarWordsTest {
  @Test
  def testRun: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_similarword.data").toURI

    val features = SimilarWords.extract(col).toArray

    assertEquals(2, features.length)
    assertEquals("ratio", features(0).name)
    assertEquals(67 / 69.0, features(0).value, 0.001)
    assertEquals("value", features(1).name)
    assertEquals(67, features(1).value, 0.001)
  }


  @Test
  def testBlockSimilarWordsScanBlock: Unit = {
    val p = BlockSimilarWords.p
    //    var r: Long = Math.abs(Random.nextLong()) % BlockSimilarWords.p
    val r = 1897516608l
    var rp = new Array[Long](msgSize);
    rp(0) = 1l
    for (i <- 1 until msgSize) {
      rp(i) = (rp(i - 1) * r) % BlockSimilarWords.p
    }

    var alphabet = Array[Char]('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')

    var lengths = Array(424)
    for (length <- lengths) {
      //      val buffer = new Array[Byte](length)
      //      for (i <- 0 until length) {
      //        buffer(i) = alphabet(Random.nextInt(alphabet.length)).asInstanceOf[Byte]
      //        //        buffer(i) = alphabet(i*3811 % alphabet.length).asInstanceOf[Byte]
      //      }
      //      val str = buffer.map(c => c.asInstanceOf[Char].toString).mkString("")
      val str = "fcahbgdedeeddcgffacbbagdafhfcfdaggfcbcgbahdgacccfheeccgedfbbbcgghahhafhbacbcehbaffahhacdhfbgdgbfhdaegdhggdacccaeheafebbfhabaeehgagcfbhafhhhdebcggggegbffdechadebhdbcfgbehbdffcegedbcbhahgdgccchdahfagdffgfhchafbaddgfahbadecdgbecebcachdhchfghdeccfdbbaecahbeeaccacfheeafbfafdhbecdchgcdchfhaehbdchfdeacfahfdfcfcgheacacacfedhcadffaddahadehgbgfeceaceaecbaafhbeddecbdhfcedafahfbhghhfehafhdbfabfhcehdcgeegcgegfhgchdcfdffbffdbgdhfbfeea"
      val buffer = str.toCharArray.map(c => c.toByte)

      val substrs = new mutable.HashSet[String]()

      for (i <- 0 until buffer.length) {
        for (j <- i + 1 to Math.min(buffer.length, i + BlockSimilarWords.msgSize)) {
          var substr = str.substring(i, j)
          val fp = fingerprint(substr, rp, p)
          if (fp == 2344344983l) {
            println("fp:" + substr)
          }
          substrs += substr
        }
      }
      val counter = substrs.groupBy(s => s.length)
      val lengthCounter = counter.map(p => (p._1, p._2.size))
      val expected = counter.map(k => k._1 * k._2.size).sum.toDouble


      val realCounter = BlockSimilarWords.scanBlock2(buffer, buffer.length, rp)

      for (i <- 1 until realCounter.length) {
        if (lengthCounter.get(i).getOrElse(0) != realCounter(i)) {
          // Look for substrings with same fingerprint
          val cand = counter.get(i).get.toList
          var found = false
          for (m <- 0 until cand.size) {
            for (n <- m + 1 until cand.size) {
              if (fingerprint(cand(m), rp, p) == fingerprint(cand(n), rp, p)) {
                print(m)
                print(n)
                found = true
              }
            }
          }
          if (!found) {
            println(r)
            println(str)
            println(lengthCounter.get(i))
            println(realCounter(i))
          }
          assertTrue(length.toString + ":" + i.toString, found)
        }
      }
    }
  }

  def fingerprint(s: String, rp: Array[Long], p: Long): Long = {
    s.zipWithIndex.map(pair => pair._1 * rp(pair._2) % p).reduce((x1, x2) => (x1 + x2) % p)
  }
}