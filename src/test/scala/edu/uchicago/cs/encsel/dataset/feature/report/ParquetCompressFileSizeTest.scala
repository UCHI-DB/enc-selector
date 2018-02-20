package edu.uchicago.cs.encsel.dataset.feature.report

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

class ParquetCompressFileSizeTest {

  @Test
  def testExtractInt: Unit = {
    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
      col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }

  @Test
  def testExtractString: Unit = {
    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
      col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }

  @Test
  def testExtractLong: Unit = {
    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
      col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }

  @Test
  def testExtractDouble: Unit = {
    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
      col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }

  @Test
  def testExtractBoolean: Unit = {

    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.BOOLEAN)
      col.colFile = new File("src/test/resource/coldata/test_col_boolean.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }

  @Test
  def testExtractFloat: Unit = {
    if (System.getProperty("os.name").equals("Linux")) {
      val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.FLOAT)
      col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

      val feature = ParquetCompressFileSize.extract(col)
      assertEquals(15, feature.size)
      val fa = feature.toArray
    }
  }
}
