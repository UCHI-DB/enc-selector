package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PIntRange, PSeq, PToken, PUnion}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/24/17.
  */
class IntegerRangeRuleTest {

  @Test
  def testRewrite: Unit = {

    val pattern = new PSeq(
      new PToken(new TInt("323")),
      new PUnion(
        new PToken(new TInt("43")),
        new PToken(new TInt("332")),
        new PToken(new TInt("242"))
      ),
      new PUnion(
        new PToken(new TWord("wm")),
        new PToken(new TInt("24"))
      )
    )
    val rule = new IntegerRangeRule
    val rewritten = rule.rewrite(pattern)

    assertTrue(rule.happened)

    assertTrue(rewritten.isInstanceOf[PSeq])

    val content = rewritten.asInstanceOf[PSeq].content

    assertEquals(3, content.length)

    assertTrue(content(1).isInstanceOf[PIntRange])
    val range = content(1).asInstanceOf[PIntRange]

    assertEquals(43, range.min)
    assertEquals(332, range.max)
  }

}
