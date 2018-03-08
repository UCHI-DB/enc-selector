package edu.uchicago.cs.encsel.ptnmining.compose

import org.junit.Assert._
import org.junit.Test

class RegexParserTest {

  @Test
  def testParse: Unit = {
    val regex = "^MIR-([0-9a-fA-F]+)-([0-9a-fA-F]+)-(\\d+)(-)?(\\d*)$"

    val parsed = RegexParser.parse(regex)

    assertEquals(13, parsed.length)
    assertEquals(0, parsed(5).rep)
    assertEquals(0, parsed(6).rep)
    assertEquals(1, parsed(10).rep)
    assertEquals(2, parsed(11).asInstanceOf[GroupToken].children.last.rep)
  }

  @Test
  def testParse2: Unit = {
    val regex = "^(\\d+)-(\\d+)-(\\d+)\\s+(\\d+):(\\d+):(\\d+\\.?\\d*)$"
    val parsed = RegexParser.parse(regex)

    assertEquals(13, parsed.length)
    assertEquals(3, parsed(6).rep)
    assertEquals(true, parsed(6).asInstanceOf[SimpleToken].escape)
  }
}
