package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/25/17.
  */
class MergeSeqRuleTest {

  @Test
  def testRewrite: Unit = {

    val pattern = PSeq.collect(
      PSeq.collect(
        new PToken(new TInt("323")),
        new PToken(new TWord("sdds"))
      ),
      PSeq.collect(
        new PToken(new TWord("Aad")),
        new PToken(new TInt("23423"))
      )
    )

    val rule = new MergeSeqRule

    val rewritten = rule.rewrite(pattern)

    assertTrue(rule.happened)

    assertTrue(rewritten.isInstanceOf[PSeq])
    val seq = rewritten.asInstanceOf[PSeq]
    assertEquals(4, seq.content.length)

    assertEquals(new TInt("323"), seq.content(0).asInstanceOf[PToken].token)
    assertEquals(new TWord("sdds"), seq.content(1).asInstanceOf[PToken].token)
  }

  @Test
  def testRewrite2: Unit = {

    val pattern = PSeq.collect(
      PSeq.collect(
        new PToken(new TInt("323")),
        new PToken(new TWord("sdds"))
      ),
      new PToken(new TWord("Aad")),
      new PToken(new TInt("23423"))
    )

    val rule = new MergeSeqRule

    val rewritten = rule.rewrite(pattern)

    assertTrue(rule.happened)

    assertTrue(rewritten.isInstanceOf[PSeq])
    val seq = rewritten.asInstanceOf[PSeq]
    assertEquals(4, seq.content.length)

    assertEquals(new TInt("323"), seq.content(0).asInstanceOf[PToken].token)
    assertEquals(new TWord("sdds"), seq.content(1).asInstanceOf[PToken].token)
  }

  @Test
  def testRewriteNotHappen: Unit = {

    val pattern = PSeq.collect(
      new PToken(new TInt("323")),
      new PToken(new TWord("sdds")),
      new PToken(new TWord("Aad")),
      new PToken(new TInt("23423"))
    )

    val rule = new MergeSeqRule

    val rewritten = rule.rewrite(pattern)

    assertFalse(rule.happened)
  }
}
