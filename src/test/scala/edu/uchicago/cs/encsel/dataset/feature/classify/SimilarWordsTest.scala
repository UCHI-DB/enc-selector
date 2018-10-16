package edu.uchicago.cs.encsel.dataset.feature.classify

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Assert._
import org.junit.Test

class SimilarWordsTest {
  @Test
  def testRun: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_similarword.data").toURI

    val features = SimilarWords.extract(col).toArray

    assertEquals(2, features.length)
    assertEquals("ratio", features(0).name)
    assertEquals(67/69.0, features(0).value, 0.001)
    assertEquals("value", features(1).name)
    assertEquals(67, features(1).value, 0.001)
  }


}