package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord, Tokenizer}
import edu.uchicago.cs.encsel.ptnmining.{PEmpty, PSeq, PToken, PUnion}
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

  @Test
  def testRewriteWithEmptyLine: Unit = {
    val data = PUnion.make(Array("2010-01-35", "2012", "", "2010-03-07", "2021-12-12", "2013-09")
      .map(s => PSeq.make(Tokenizer.tokenize(s).toList.map(new PToken(_)))))
    val rule = new CommonSymbolRule()
    val rr = rule.rewrite(data)

    assertTrue(rule.happened)

    assertTrue(rr.isInstanceOf[PUnion])
    val u = rr.asInstanceOf[PUnion]

    assertEquals(PEmpty, u.content(1))
    assertTrue(u.content(0).isInstanceOf[PSeq])

    val seq = u.content(0).asInstanceOf[PSeq]

    assertEquals(3, seq.content.size)

    assertTrue(seq.content(0).isInstanceOf[PUnion])
    val u0 = seq.content(0).asInstanceOf[PUnion]
    assertEquals(4, u0.content.size)
    assertTrue(u0.content.contains(new PToken(new TInt("2010"))))
    assertTrue(u0.content.contains(new PToken(new TInt("2012"))))
    assertTrue(u0.content.contains(new PToken(new TInt("2021"))))
    assertTrue(u0.content.contains(new PToken(new TInt("2013"))))

    assertTrue(seq.content(1).isInstanceOf[PUnion])
    val u1 = seq.content(1).asInstanceOf[PUnion]
    assertEquals(2, u1.content.size)
    assertTrue(u1.content.contains(new PToken(new TSymbol("-"))))
    assertTrue(u1.content.contains(PEmpty))

    assertTrue(seq.content(2).isInstanceOf[PUnion])
    val u2 = seq.content(2).asInstanceOf[PUnion]
    assertEquals(5, u2.content.size)

    assertTrue(u2.content.contains(PEmpty))
    assertTrue(u2.content.contains(new PToken(new TInt("09"))))

    val s20 = PSeq.make(Seq(
      new PToken(new TInt("03")),
      new PToken(new TSymbol("-")),
      new PToken(new TInt("07"))))
    assertTrue(u2.content.contains(s20))

    val s21 = PSeq.make(Seq(
      new PToken(new TInt("12")),
      new PToken(new TSymbol("-")),
      new PToken(new TInt("12"))))
    assertTrue(u2.content.contains(s21))

    val s22 = PSeq.make(Seq(
      new PToken(new TInt("01")),
      new PToken(new TSymbol("-")),
      new PToken(new TInt("35"))))
    assertTrue(u2.content.contains(s22))
  }
}
