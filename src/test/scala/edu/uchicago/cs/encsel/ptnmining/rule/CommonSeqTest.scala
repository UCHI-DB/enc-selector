package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TWord, Token, Tokenizer}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/14/17.
  */
class CommonSeqTest {

  @Test
  def testBetween: Unit = {
    val a = "778-9383 Suspendisse Av. Weirton IN 93479 (326) 677-3419"
    val b = "Ap #285-7193 Ullamcorper Avenue Amesbury HI 93373 (302) 259-2375"

    val atokens = Tokenizer.tokenize(a).toSeq
    val btokens = Tokenizer.tokenize(b).toSeq

    val cseq = new CommonSeq

    val commons = cseq.between(atokens, btokens, (a: Token, b: Token) => {
      (a.getClass == b.getClass) && (a match {
        case wd: TWord => a.value.equals(b.value)
        case _ => true
      })
    })

    assertEquals(5, commons.size)

    //noinspection ZeroIndexToHead
    assertEquals((0, 3, 4), commons(0))
    assertEquals((5, 1, 1), commons(1))
    assertEquals((7, 8, 1), commons(2))
    assertEquals((9, 10, 1), commons(3))
    assertEquals((11, 14, 8), commons(4))
  }

  @Test
  def testFind: Unit = {
    val a = Array(Array(1, 2, 3, 4, 5, 6, 7, 8, 9),
      Array(1, 2, 3, 4, 9, 5, 6, 2),
      Array(2, 3, 4, 4, 2, 5, 6, 1),
      Array(3, 4, 5, 6, 7)).map(_.toSeq).toSeq

    val cseq = new CommonSeq
    val commons = cseq.find(a, (a: Int, b: Int) => {
      a == b
    })

    assertEquals(2, commons.length)
    assertArrayEquals(Array(3, 4), commons(0).toArray)
    assertArrayEquals(Array(5, 6), commons(1).toArray)

    val pos = cseq.positions
    assertEquals(4, pos.length)

    assertEquals((2, 2), pos(0)(0))
    assertEquals((4, 2), pos(0)(1))
    assertEquals((2, 2), pos(1)(0))
    assertEquals((5, 2), pos(1)(1))
    assertEquals((1, 2), pos(2)(0))
    assertEquals((5, 2), pos(2)(1))
    assertEquals((0, 2), pos(3)(0))
    assertEquals((2, 2), pos(3)(1))
  }
}

