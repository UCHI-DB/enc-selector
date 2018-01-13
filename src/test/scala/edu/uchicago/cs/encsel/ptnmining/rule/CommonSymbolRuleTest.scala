package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion}
import org.junit.Assert._
import org.junit.Test

class CommonSymbolRuleTest {


  @Test
  def testRewrite: Unit = {
    val union = new PUnion(
      new PSeq(new PToken(new TWord("abc")), new PToken(new TInt("312")),
        new PToken(new TSymbol("-")), new PToken(new TInt("212")),
        new PToken(new TWord("good")), new PToken(new TSymbol("-"))),
      new PSeq(new PToken(new TInt("4021")), new PToken(new TSymbol("-")),
        new PToken(new TSymbol("-")), new PToken(new TInt("420"))),
      new PSeq(new PToken(new TWord("kwmt")), new PToken(new TWord("ddmpt")),
        new PToken(new TInt("2323")), new PToken(new TSymbol("-")),
        new PToken(new TSymbol("-"))),
      new PSeq(new PToken(new TWord("ttpt")), new PToken(new TInt("3232")),
        new PToken(new TSymbol("-")), new PToken(new TInt("42429")),
        new PToken(new TWord("dddd")), new PToken(new TSymbol("-"))))

    val rule = new CommonSymbolRule()
    val result = rule.rewrite(union)

    assertTrue(rule.happened)

    assertTrue(result.isInstanceOf[PSeq])

    val seq = result.asInstanceOf[PSeq]

    assertEquals(5, seq.content.length)

    assertTrue(seq.content(0).isInstanceOf[PUnion])
    val u0 = seq.content(0).asInstanceOf[PUnion]
    assertEquals(4, u0.content.size)

    assertTrue(seq.content(1).isInstanceOf[PToken])
    val t0 = seq.content(1).asInstanceOf[PToken]
    assertEquals("-", t0.token.toString)

    assertTrue(seq.content(2).isInstanceOf[PUnion])
    val u1 = seq.content(2).asInstanceOf[PUnion]
    assertEquals(3, u1.content.size)

    assertTrue(seq.content(3).isInstanceOf[PToken])
    val t1 = seq.content(3).asInstanceOf[PToken]
    assertEquals("-", t1.token.toString)

    assertTrue(seq.content(4).isInstanceOf[PUnion])
    val u2 = seq.content(4).asInstanceOf[PUnion]
    assertEquals(2, u2.content.size)
  }
}
