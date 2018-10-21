package edu.uchicago.cs.encsel.dataset.feature.classify

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class SimilarWordsTest {
    @Test
    def testRun: Unit = {
      val col = new Column(null, -1, "", DataType.INTEGER)
      col.colFile = new File("src/test/resource/coldata/test_col_similarword.data").toURI

      val features = new SimilarWords().extract(col).toArray

      assertEquals(4, features.length)
      assertEquals("ratio", features(0).name)
      assertEquals(0.174, features(0).value, 0.001)
      assertEquals("msglen_entropy", features(1).name)
      assertEquals(1.907, features(1).value, 0.001)
      assertEquals("dist_entropy", features(2).name)
      assertEquals(2.485, features(2).value, 0.001)
      assertEquals("char_entropy", features(3).name)
      assertEquals(0.888, features(3).value, 0.001)
    }


  @Test
  def testBlockSimilarWordsScanBlock: Unit = {

    val bsw = new SimilarWords()
    val fpr = new Fingerprint(bsw.msgSize)

    var string: Array[Byte] = null
    var info: BlockInfo = null

    string = "ababaababbaababa".getBytes(StandardCharsets.UTF_8);
    info = new SimilarWords().scanBlock(string, string.length, fpr)
    assertEquals(5.0 / string.length, info.compressionRatio, 0.001);
    assertEquals(1.055, info.msglenEntropy, 0.001)
    assertEquals(1.332, info.msgDistEntropy, 0.001)
    assertEquals(0.673, info.charEntropy, 0.001)

    string = "ababaababbaabab".getBytes(StandardCharsets.UTF_8);
    info = new SimilarWords().scanBlock(string, string.length, fpr)
    assertEquals(5.0 / string.length, info.compressionRatio, 0.001);
    assertEquals(1.055, info.msglenEntropy, 0.001)
    assertEquals(1.332, info.msgDistEntropy, 0.001)
    assertEquals(1.055, info.charEntropy, 0.001)

    string = "badadadabaab".getBytes(StandardCharsets.UTF_8);
    info = new SimilarWords().scanBlock(string, string.length, fpr)
    assertEquals(6.0 / string.length, info.compressionRatio, 0.001);
    assertEquals(0.868, info.msglenEntropy, 0.001)
    assertEquals(1.242, info.msgDistEntropy, 0.001)
    assertEquals(1.330, info.charEntropy, 0.001)
  }


  @Test
  def testBlockSize: Unit = {

    val msgSize = 4
    val fpr = new Fingerprint(msgSize)

    var string = "ababaababbaababa".getBytes(StandardCharsets.UTF_8);
    var info = new SimilarWords(msgSize).scanBlock(string, string.length, fpr)
    assertEquals(6.0 / string.length, info.compressionRatio, 0.001)
    assertEquals(1.011, info.msglenEntropy, 0.001)
    assertEquals(1.561, info.msgDistEntropy, 0.001)
    assertEquals(1.099, info.charEntropy, 0.001)

    string = "ababaababbaabab".getBytes(StandardCharsets.UTF_8);
    info = new SimilarWords(msgSize).scanBlock(string, string.length, fpr)
    assertEquals(6.0 / string.length, info.compressionRatio, 0.001)
    assertEquals(0.693, info.msglenEntropy, 0.001)
    assertEquals(1.561, info.msgDistEntropy, 0.001)
    assertEquals(1.099, info.charEntropy, 0.001)

    string = "badadadabaab".getBytes(StandardCharsets.UTF_8);
    info = new SimilarWords(msgSize).scanBlock(string, string.length, fpr)
    assertEquals(7.0 / string.length, info.compressionRatio, 0.001)
    assertEquals(0.956, info.msglenEntropy, 0.001)
    assertEquals(1.475, info.msgDistEntropy, 0.001)
    assertEquals(1.352, info.charEntropy, 0.001)
  }
}

class FingerprintTest {
  @Test
  def testInverse = {
    val fp = new Fingerprint

    val inv = fp.inverse
    assertTrue(inv > 0)
    assertEquals(1, inv * fp.r % fp.p)
  }

  @Test
  def testCombine = {
    for (i <- 0 to 1000) {
      val fp = new Fingerprint
      val teststr = UUID.randomUUID().toString
      val fullfp = fp.get(teststr)

      val suffixlen = Random.nextInt(teststr.length - 4) + 4

      val prefix = teststr.substring(0, teststr.length - suffixlen)
      val suffix = teststr.substring(teststr.length - suffixlen)
      val prefixfp = fp.get(prefix)
      val suffixfp = fp.get(suffix)
      assertEquals(fullfp, fp.combine(prefixfp, prefix.length, suffixfp))
    }
  }

  @Test
  def testDivide = {
    val fp = new Fingerprint

    val teststr = UUID.randomUUID().toString
    val fullfp = fp.get(teststr)

    val suffixlen = Random.nextInt(teststr.length - 4) + 4

    val prefix = teststr.substring(0, teststr.length - suffixlen)
    val suffix = teststr.substring(teststr.length - suffixlen)
    val prefixfp = fp.get(prefix)
    val suffixfp = fp.get(suffix)

    assertEquals(suffixfp, fp.divide(fullfp + fp.p - prefixfp, prefix.length))
  }
}