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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion}
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord, Tokenizer}
import org.junit.Test
import org.junit.Assert._

class SameLenMergeRuleTest {

  @Test
  def testRewrite: Unit = {
    val data = PUnion(Array("3A5FB", "45527", "667FD", "73982", "A3A2D")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val output = rule.rewrite(data)
    assertTrue(rule.happened)

    assertTrue(output.asInstanceOf[PUnion].content.forall(p => p.asInstanceOf[PToken].token.isInstanceOf[TInt]))
  }

  @Test
  def testRewrite2: Unit = {
    val ptn = PSeq.collect(
      new PToken(new TInt("324")),
      new PToken(new TWord("dasf")),
      new PToken(new TInt("323")))
    val rule = new SameLenMergeRule
    val result = rule.rewrite(ptn)

    assertFalse(rule.happened)

    val ptn2 = PSeq.collect(new PToken(new TInt("3")),
      new PToken(new TWord("A")),
      new PToken(new TInt("5")),
      new PToken(new TWord("FB")))
    rule.reset
    val result2 = rule.rewrite(ptn2)

    assertFalse(rule.happened)
  }
}
