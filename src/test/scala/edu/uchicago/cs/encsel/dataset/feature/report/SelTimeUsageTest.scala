package edu.uchicago.cs.encsel.dataset.feature.report

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

class SelTimeUsageTest {

  @Test
  def testExtractInt:Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = SelTimeUsage.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray

  }

  @Test
  def testExtractString:Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = SelTimeUsage.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray
  }
}
