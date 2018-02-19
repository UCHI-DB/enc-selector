package edu.uchicago.cs.encsel.dataset.feature

import org.junit.Test
import org.junit.Assert._
import java.io.File
import ch.qos.logback.core.util.FileSize
import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType

class EncFileSizeTest {

  @Test
  def testExtractInt: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(5, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(327.0, fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(354.0, fa(1).value, 0.001)

    assertTrue(fa(2).featureType.equals("EncFileSize"))
    assertEquals("BP_file_size", fa(2).name)
    assertEquals(317.0, fa(2).value, 0.001)

    assertTrue(fa(3).featureType.equals("EncFileSize"))
    assertEquals("RLE_file_size", fa(3).name)
    assertEquals(331.0, fa(3).value, 0.001)

    assertTrue(fa(4).featureType.equals("EncFileSize"))
    assertEquals("DELTABP_file_size", fa(4).name)
    assertEquals(398.0, fa(4).value, 0.001)
  }

  @Test
  def testExtractLong: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(3, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(412.0, fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(443.0, fa(1).value, 0.001)

    assertTrue(fa(2).featureType.equals("EncFileSize"))
    assertEquals("DELTABP_file_size", fa(2).name)
    assertEquals(412.0, fa(2).value, 0.001)

  }

  @Test
  def testExtractDouble: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(2, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(452.0, fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(483.0, fa(1).value, 0.001)

  }
}