package edu.uchicago.cs.encsel.dataset.parser.csv

import java.io.File

import org.junit.Test
import org.junit.Assert._
import edu.uchicago.cs.encsel.dataset.schema.Schema
import java.util.Arrays

import edu.uchicago.cs.encsel.dataset.parser.ParserFactory

class CommonsCSVParserTest {

  @Test
  def testParse(): Unit = {
    val records = new CommonsCSVParser().parse(new File("src/test/resource/filefmt/test_csv_parser.csv").toURI,
      Schema.fromParquetFile(new File("src/test/resource/filefmt/test_csv_parser.schema").toURI)).toArray
    assertEquals(7, records.length)
    assertEquals("""What a said "Not Good"""", records(0)(1))
  }


  @Test
  def testHeader:Unit = {
    val headerParser = ParserFactory.getParser(new File("src/test/resource/filefmt/header.csv").toURI)
    val headlessParser = ParserFactory.getParser(new File("src/test/resource/filefmt/headerless.csv").toURI)

    assertTrue(headerParser.hasHeaderInFile)
    assertFalse(headlessParser.hasHeaderInFile)
  }
}