package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/29/17.
  */
class CommonSeqRuleTest {

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
    val csq = new CommonSeqRule

    val newptn = csq.rewrite(union)

    assertTrue(csq.happened)

    assertTrue(newptn.isInstanceOf[PSeq])
    val newseq = newptn.asInstanceOf[PSeq]

    assertEquals(5, newseq.content.length)

    //noinspection ZeroIndexToHead
    assertTrue(newseq.content(0).isInstanceOf[PUnion])
    assertTrue(newseq.content(1).isInstanceOf[PToken])
    assertTrue(newseq.content(2).isInstanceOf[PUnion])
    assertTrue(newseq.content(3).isInstanceOf[PToken])
    assertTrue(newseq.content(4).isInstanceOf[PUnion])

    val union1 = newseq.content.head.asInstanceOf[PUnion]
    val common = newseq.content(1).asInstanceOf[PToken]
    val union2 = newseq.content(2).asInstanceOf[PUnion]
    val common2 = newseq.content(3).asInstanceOf[PToken]

    assertEquals(4, union1.content.size)
    assertEquals(new TSymbol("-"), common.token)
    assertEquals(new TSymbol("-"), common.token)
    assertEquals(4, union2.content.size)

    val u1c = union1.content.toArray
    assertEquals(u1c(0), new PSeq(new PToken(new TWord("abc")), new PToken(new TInt("312"))))
    assertEquals(u1c(1), new PToken(new TInt("4021")))
    assertEquals(u1c(2), new PSeq(new PToken(new TWord("kwmt")), new PToken(new TWord("ddmpt")), new PToken(new TInt("2323"))))
    assertEquals(u1c(3), new PSeq(new PToken(new TWord("ttpt")), new PToken(new TInt("3232"))))
  }

  @Test
  def testWithSimilarWord:Unit = {

  }
}
