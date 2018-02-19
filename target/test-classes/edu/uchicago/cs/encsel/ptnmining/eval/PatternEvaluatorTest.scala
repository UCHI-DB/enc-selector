package edu.uchicago.cs.encsel.ptnmining.eval

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord, Tokenizer}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/6/17.
  */
class PatternEvaluatorTest {

  @Test
  def testEvaluate: Unit = {
    // Pattern Size is 63
    val pattern1 = new PSeq(
      new PToken(new TWord("good")),
      new PUnion(
        new PToken(new TInt("1")),
        new PToken(new TInt("2")),
        new PToken(new TInt("3"))
      ),
      new PToken(new TSymbol("-")),
      new PUnion(
        new PToken(new TWord("ASM")),
        new PToken(new TWord("MTM")),
        new PToken(new TWord("DDTE")),
        new PSeq(
          new PToken(new TWord("CHO")),
          new PToken(new TSymbol("-")),
          new PUnion(
            new PToken(new TWord("A")),
            new PToken(new TWord("B")),
            new PToken(new TWord("C")),
            new PToken(new TWord("D"))
          )
        )
      ),
      new PIntRange(100, 150)
    )

    val dataset = Seq("good1-ASM127", "good2-MTM101", "good2-DDTE122",
      "good3-CHO-A134", "good3-CHO-B120", "good3-ASM149", "ttmdpt-dawee-323").map(Tokenizer.tokenize(_).toSeq)

    val result = PatternEvaluator.evaluate(pattern1, dataset)

    assertEquals(99, result, 0.01)

    val pattern2 = new PSeq(new PWordAny, new PIntAny, new PToken(new TSymbol("-")),
      new PWordAny,
      new PUnion(PEmpty, new PSeq(new PWordAny, new PToken(new TSymbol("-")), new PWordAny)),
      new PIntAny)

    val result2 = PatternEvaluator.evaluate(pattern2, dataset)

    assertEquals(143, result2, 0.01)
  }
}
