package edu.uchicago.cs.encsel.dataset.feature

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.report.ScanTimeUsage
import edu.uchicago.cs.encsel.model.{DataType, IntEncoding}
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import org.junit.Assert._
import org.junit.{BeforeClass, Test}

object ScanTimeUsageTest {
  @BeforeClass
  def encodeFile: Unit = {
    val file = new File("src/test/resource/scantime/data").toURI

    IntEncoding.values.filter(_.parquetEncoding() != null).foreach(enc => {
      ParquetWriterHelper.singleColumnInt(file, enc)
    })

  }
}

class ScanTimeUsageTest {
  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/scantime/data").toURI
    val features = ScanTimeUsage.extract(col).toArray

    assertEquals(15, features.size);

    // TODO More checks
  }
}
