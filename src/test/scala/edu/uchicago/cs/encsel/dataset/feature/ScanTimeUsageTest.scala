package edu.uchicago.cs.encsel.dataset.feature

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import org.junit.Test
import org.junit.Assert._

class ScanTimeUsageTest {

  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/scantime/STREETSEGID_65120090766779833858.tmp").toURI
    val features = ScanTimeUsage.extract(col).toArray

    assertEquals(10,features.size);
  }
}
