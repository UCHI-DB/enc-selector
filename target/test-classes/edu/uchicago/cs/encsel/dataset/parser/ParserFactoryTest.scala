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
    val csvParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.csv").toURI)
    assertTrue(csvParser.isInstanceOf[CommonsCSVParser])
    
    val jsonParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.json").toURI)
    assertTrue(jsonParser.isInstanceOf[LineJsonParser])
    
    val xlsxParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.xlsx").toURI)
    assertTrue(xlsxParser.isInstanceOf[XLSXParser])
    
    val tsvParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.tsv").toURI)
    assertTrue(tsvParser.isInstanceOf[TSVParser])
  }
}