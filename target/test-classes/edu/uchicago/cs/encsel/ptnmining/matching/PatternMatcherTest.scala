/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.ptnmining.matching

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/1/17.
  */
class PatternMatcherTest {

  @Test
  def testMatchon: Unit = {

    val pattern = new PSeq(
      new PSeq(new PToken(new TWord("mmtm")), new PToken(new TWord("wwkp"))),
      new PUnion(
        new PSeq(new PToken(new TWord("nmsmd")), new PWordAny, new PIntAny),
        new PSeq(
          new PToken(new TInt("3232")),
          new PUnion(
            new PSeq(new PToken(new TWord("dassd")), new PToken(new TInt("34223"))),
            new PSeq(new PToken(new TWord("mmd")), new PToken(new TWord("wwtm"))),
            new PToken(new TWord("www"))
          )
        )
      )
    )

    pattern.naming()


    var record = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TInt("3232"), new TWord("mmd"), new TWord("wwtm")))
    assertTrue(record.isDefined)
    val rec = record.get
    assertEquals(new TWord("mmtm"), rec.get("_0_0_0").get)
    assertEquals(new TWord("wwkp"), rec.get("_0_0_1").get)
    assertEquals(new TInt("3232"), rec.get("_0_1_1_0").get)
    assertEquals(new TWord("mmd"), rec.get("_0_1_1_1_1_0").get)
    assertEquals(new TWord("wwtm"), rec.get("_0_1_1_1_1_1").get)
    assertTrue(rec.get("_0_1_0_0").isEmpty)
    assertTrue(rec.get("_0_1_0_1").isEmpty)

    record = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TWord("nmsmd"), new TWord("mmd"), new TInt("312")))
    assertTrue(record.isDefined)

    record = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TInt("3232"), new TWord("dassd"), new TInt("34223")))
    assertTrue(record.isDefined)

    record = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TInt("3232"), new TWord("www")))
    assertTrue(record.isDefined)

  }

  @Test
  def testMatchItems: Unit = {

    val patterns = Seq(new PWordAny,
      new PIntAny,
      new PToken(new TWord("aab")),
      new PToken(new TWord("wtw")),
      PEmpty,
      new PWordAny,
      new PIntRange(3, 60))
    patterns.zipWithIndex.foreach(p => p._1.name = p._2.toString)

    val rec1 = PatternMatcher.matchItems(patterns,
      Seq(new TWord("dkkd"), new TInt("3123"), new TWord("aab"), new TWord("wtw"),
        new TWord("kmpt"), new TInt("21")))
    assertTrue(rec1.isDefined)

    assertEquals(new TWord("dkkd"), rec1.get.get("0").get)
    assertEquals(new TInt("3123"), rec1.get.get("1").get)
    assertEquals(new TWord("aab"), rec1.get.get("2").get)
    assertEquals(new TWord("wtw"), rec1.get.get("3").get)
    assertEquals(new TWord("kmpt"), rec1.get.get("5").get)
    assertEquals(new TInt("21"), rec1.get.get("6").get)

    val rec2 = PatternMatcher.matchItems(patterns,
      Seq(new TInt("3432"), new TWord("kkmdpt"), new TWord("wpnta")))
    assertTrue(rec2.isEmpty)

    val rec3 = PatternMatcher.matchItems(patterns,
      Seq(new TWord("dkkd"), new TInt("3123"), new TWord("aab"), new TWord("wtw"),
        new TWord("kmpt"), new TInt("99"))
    )
    assertTrue(rec3.isEmpty)
  }

  @Test
  def testChoice: Unit = {
    val pattern = new PSeq(
      new PSeq(new PToken(new TWord("mmtm")), new PToken(new TWord("wwkp"))),
      new PUnion(
        new PSeq(new PToken(new TWord("nmsmd")), new PWordAny, new PIntAny),
        new PSeq(
          new PToken(new TInt("3232")),
          new PUnion(
            new PSeq(new PToken(new TWord("dassd")), new PToken(new TInt("34223"))),
            new PSeq(new PToken(new TWord("mmd")), new PToken(new TWord("wwtm"))),
            new PSeq(new PToken(new TWord("www")),
              new PUnion(
                new PToken(new TWord("aad")),
                new PToken(new TInt("334"))
              )
            )
          )
        )
      )
    )

    pattern.naming()


    val record = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TInt("3232"), new TWord("mmd"), new TWord("wwtm")))

    assertTrue(record.isDefined)

    val rec = record.get

    assertEquals(1, rec.choices.getOrElse("_0_1", (-1, -1))._1)
    assertEquals(1, rec.choices.getOrElse("_0_1_1_1", (-1, -1))._1)
    assertEquals(2, rec.choices.size)
    assertFalse(rec.choices.contains("_0_1_1_1_2_1"))

    val record2 = pattern.matchon(Seq(new TWord("mmtm"), new TWord("wwkp"),
      new TInt("3232"), new TWord("www"), new TWord("aad")))

    assertTrue(record2.isDefined)

    val rec2 = record2.get

    assertEquals(1, rec2.choices.getOrElse("_0_1", (-1, -1))._1)
    assertEquals(2, rec2.choices.getOrElse("_0_1_1_1", (-1, -1))._1)
    assertEquals(3, rec2.choices.size)
    assertEquals(0, rec2.choices.getOrElse("_0_1_1_1_2_1", (-1, -1))._1)
  }
}
