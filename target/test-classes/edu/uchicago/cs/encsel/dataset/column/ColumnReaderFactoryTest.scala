package edu.uchicago.cs.encsel.dataset.column

import java.io.File

import org.junit.Assert.assertTrue
import org.junit.Test

import edu.uchicago.cs.encsel.dataset.column.csv.CSVColumnReader2
import edu.uchicago.cs.encsel.dataset.column.json.JsonColumnReader
import edu.uchicago.cs.encsel.dataset.column.tsv.TSVColumnReader

class ColumnReaderFactoryTest {
  @Test
  def testGetColumnReader(): Unit = {
    var cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_columner.csv").toURI)
    assertTrue(cr.isInstanceOf[CSVColumnReader2])

    cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_columner.tsv").toURI)
    assertTrue(cr.isInstanceOf[TSVColumnReader])
    
    cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_json_parser.json").toURI)
    assertTrue(cr.isInstanceOf[JsonColumnReader])
  }
}