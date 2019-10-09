package edu.uchicago.cs.encsel.dataset.parser

import org.junit.Test
import org.junit.Assert._
import java.io.File

import edu.uchicago.cs.encsel.dataset.parser.csv.CommonsCSVParser
import edu.uchicago.cs.encsel.dataset.parser.excel.XLSXParser
import edu.uchicago.cs.encsel.dataset.parser.tsv.TSVParser
import edu.uchicago.cs.encsel.dataset.parser.json.LineJsonParser

class ParserFactoryTest {

  @Test
  def testGetParser(): Unit = {
    val csvParser = ParserFactory.getParser(new File("src/test/resource/filefmt/test_csv_parser.csv").toURI)
    assertTrue(csvParser.isInstanceOf[CommonsCSVParser])

    val jsonParser = ParserFactory.getParser(new File("src/test/resource/filefmt/test_json_parser.json").toURI)
    assertTrue(jsonParser.isInstanceOf[LineJsonParser])

    val xlsxParser = ParserFactory.getParser(new File("src/test/resource/filefmt/test_xlsx_parser.xlsx").toURI)
    assertTrue(xlsxParser.isInstanceOf[XLSXParser])

    val tsvParser = ParserFactory.getParser(new File("src/test/resource/filefmt/test_tsv_parser.tsv").toURI)
    assertTrue(tsvParser.isInstanceOf[CommonsCSVParser])
  }

  @Test
  def testConfigCSVParser: Unit = {
    //    val headerParser = ParserFactory.getParser(new File("src/test/resource/filefmt/header.csv").toURI)
    val headlessParser = ParserFactory.getParser(new File("src/test/resource/filefmt/headerless.csv").toURI)
    val headwrongParser = ParserFactory.getParser(new File("src/test/resource/filefmt/header_wrong.csv").toURI)

    //    assertEquals(24, headerParser.guessedHeader.size)
    //    for (i <- 0 until 24) {
    //      assertEquals(headerParser.guessedHeader(i), "f%d".format(i + 1))
    //    }
    val headless = headlessParser.parse(new File("src/test/resource/filefmt/headerless.csv").toURI, null).toArray
    assertEquals(10, headless.size)

    val headwrong = headwrongParser.parse(new File("src/test/resource/filefmt/header_wrong.csv").toURI, null).toArray
    assertEquals(10, headwrong.size)

    assertEquals(24, headlessParser.headerNames.size)
    for (i <- 0 until 24) {
      assertEquals("f%d".format(i),headlessParser.headerNames(i))
    }

    assertEquals(24, headwrongParser.headerNames.size)
    for (i <- 0 until 24) {
      assertEquals("f%d".format(i),headlessParser.headerNames(i))
    }

    //  val header = headerParser.parse(new File("src/test/resource/filefmt/header.csv").toURI, null).toArray
    //  assertEquals(10, header.size)

  }

}