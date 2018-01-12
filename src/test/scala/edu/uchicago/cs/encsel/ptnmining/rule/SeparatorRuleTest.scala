package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion}
import org.junit.Assert._
import org.junit.Test

class SeparatorRuleTest {


  @Test
  def testRewrite: Unit = {
    val union = new PUnion(
      new PSeq(new PToken(new TWord("abc")), new PToken(new TInt("312")),
        new PToken(new TSymbol("-")), new PToken(new TInt("212")),
        new PToken(new TWord("good")), new PToken(new TSymbol("-"))),
      new PSeq(new PToken(new TInt("4021")), new PToken(new TSymbol("-")),
        new PToken(new TInt("2213")), new PToken(new TWord("akka")),
        new PToken(new TSymbol("-")), new PToken(new TInt("420"))),
      new PSeq(new PToken(new TWord("kwmt")), new PToken(new TWord("ddmpt")),
        new PToken(new TInt("2323")), new PToken(new TSymbol("-")),
        new PToken(new TInt("33130")), new PToken(new TSymbol("-"))),
      new PSeq(new PToken(new TWord("ttpt")), new PToken(new TInt("3232")),
        new PToken(new TSymbol("-")), new PToken(new TInt("42429")),
        new PToken(new TWord("dddd")), new PToken(new TSymbol("-"))))

    val rule = new SeparatorRule()
    val result = rule.rewrite(union)

    assertTrue(result.isInstanceOf[PSeq])

    val seq = result.asInstanceOf[PSeq]

    assertEquals(5, seq.content.length)
  }
}
