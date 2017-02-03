package edu.uchicago.cs.encsel.colread.json

import java.io.File

import org.junit.Assert.assertEquals
import org.junit.Test

import edu.uchicago.cs.encsel.colread.Schema
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.colread.csv.CSVColumnReader
import scala.io.Source

class CSVColumnReaderTest {

  @Test
  def testReadColumn(): Unit = {
    var sourceFile = new File("src/test/resource/test_colreader.csv").toURI()
    var ccr = new CSVColumnReader()
    var schema = Schema.fromParquetFile(new File("src/test/resource/test_colreader.schema").toURI())
    var cols = ccr.readColumn(sourceFile, schema)

    assertEquals(5, cols.size)
    var arrays = cols.toArray

    assertEquals(0, arrays(0).colIndex)
    assertEquals("id", arrays(0).colName)
    assertEquals(DataType.INTEGER, arrays(0).dataType)
    assertEquals(sourceFile, arrays(0).origin)

    assertEquals(1, arrays(1).colIndex)
    assertEquals("c1", arrays(1).colName)
    assertEquals(DataType.BOOLEAN, arrays(1).dataType)
    assertEquals(sourceFile, arrays(1).origin)

    assertEquals(2, arrays(2).colIndex)
    assertEquals("c2", arrays(2).colName)
    assertEquals(DataType.FLOAT, arrays(2).dataType)
    assertEquals(sourceFile, arrays(2).origin)

    assertEquals(3, arrays(3).colIndex)
    assertEquals("c3", arrays(3).colName)
    assertEquals(DataType.STRING, arrays(3).dataType)
    assertEquals(sourceFile, arrays(3).origin)

    assertEquals(4, arrays(4).colIndex)
    assertEquals("c4", arrays(4).colName)
    assertEquals(DataType.INTEGER, arrays(4).dataType)
    assertEquals(sourceFile, arrays(4).origin)

    arrays.foreach { col =>
      {
        assertEquals(1, Source.fromFile(col.colFile).getLines().size)
      }
    }
  }
}