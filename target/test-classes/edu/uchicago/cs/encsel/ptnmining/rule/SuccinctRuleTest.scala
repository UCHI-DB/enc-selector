package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PEmpty, PSeq, PToken, PUnion}
import org.junit.Test
import org.junit.Assert._
/**
  * Created by harper on 3/29/17.
  */
class SuccinctRuleTest {

  @Test
  def testRewrite: Unit = {
    val pattern = new PSeq(
      new PUnion(Array(PEmpty, new PToken(new TWord("ddd")))),
      new PSeq(new PToken(new TInt("32342"))),
      new PUnion(Array(new PToken(new TWord("abc")), new PToken(new TWord("abc")))))

    val rule = new SuccinctRule

    val rewritten = rule.rewrite(pattern)

    assertTrue(rule.happened)

    assertTrue(rewritten.isInstanceOf[PSeq])
    val newseq = rewritten.asInstanceOf[PSeq]
    assertEquals(3, newseq.content.length)

    //noinspection ZeroIndexToHead
    assertTrue(newseq.content(0).isInstanceOf[PUnion])
    assertTrue(newseq.content(1).isInstanceOf[PToken])
    assertTrue(newseq.content(2).isInstanceOf[PToken])

    //noinspection ZeroIndexToHead
    assertEquals(2, newseq.content(0).asInstanceOf[PUnion].content.size)
    assertEquals("32342",newseq.content(1).asInstanceOf[PToken].token.value)
    assertEquals("abc",newseq.content(2).asInstanceOf[PToken].token.value)
  }
}
