package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.{PIntAny, PSeq, PToken, PUnion}
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord, Tokenizer}
import org.junit.Test
import org.junit.Assert._

/**
  * Created by harper on 4/5/17.
  */
class UseAnyRuleTest {

  @Test
  def testRewrite: Unit = {
    val ptn = new PSeq(
      new PToken(new TInt("23432")),
      new PUnion(
        new PToken(new TInt("323")),
        new PToken(new TInt("32322")),
        new PToken(new TInt("333")),
        new PToken(new TInt("1231"))
      ),
      new PToken(new TWord("dasfdfa")))

    val rule = new UseAnyRule
    rule.generateOn((0 to 10).map(i => String.valueOf(i)).map(Tokenizer.tokenize(_).toSeq))
    val result = rule.rewrite(ptn)
    assertTrue(rule.happened)

    assertTrue(result.isInstanceOf[PSeq])
    val resq = result.asInstanceOf[PSeq]
    assertEquals(3, resq.content.size)

    assertTrue(resq.content(0).isInstanceOf[PToken])
    assertTrue(resq.content(1).isInstanceOf[PIntAny])
    assertTrue(resq.content(2).isInstanceOf[PToken])

  }

  @Test
  def testNotHappen: Unit = {
    val ptn = new PSeq(
      new PToken(new TInt("23432")),
      new PUnion(
        new PToken(new TInt("323")),
        new PToken(new TInt("32322")),
        new PToken(new TWord("abbd")),
        new PToken(new TInt("1231"))
      ),
      new PToken(new TWord("dasfdfa")))

    val rule = new UseAnyRule
    rule.generateOn((0 to 10).map(i => String.valueOf(i)).map(Tokenizer.tokenize(_).toSeq))
    val result = rule.rewrite(ptn)
    assertFalse(rule.happened)
  }
}
