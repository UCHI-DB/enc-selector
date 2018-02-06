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

package edu.uchicago.cs.encsel.ptnmining.matching

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TSymbol, TWord}
import org.junit.Assert._
import org.junit.Test

class GenRegexVisitorTest {

  @Test
  def testVisit: Unit = {
    val ptn = PSeq.collect(
      new PIntAny(5),
      new PToken(new TWord("And")),
      new PIntAny(),
      PEmpty,
      PUnion.collect(
        PEmpty,
        new PToken(new TWord("dmd")),
        new PToken(new TInt("12312")),
        new PIntAny(3, 4)
      ),
      new PWordAny(5),
      new PToken(new TInt("3077")),
      new PDoubleAny(),
      new PWordAny(),
      new PToken(new TWord("mpq")),
      new PWordAny(10)
    )
    ptn.naming()

    val regexv = new GenRegexVisitor
    ptn.visit(regexv)
    assertEquals("^(\\d{5})And(\\d+)(dmd|12312|(\\d{3,4}))?(\\w{5})3077(\\d+\\.?\\d*)(\\w+)mpq(\\w{10})$",
      regexv.get)

    assertArrayEquals(Array[AnyRef]("_0_0", "_0_2", "_0_3", "_0_3_3", "_0_4", "_0_6", "_0_7", "_0_9"), regexv.list.toArray[AnyRef])
  }

  @Test
  def testSpecialChars: Unit = {
    val ptn = PSeq.collect(
      new PIntAny(5),
      new PToken(new TSymbol("+")),
      new PIntAny(),
      new PToken(new TSymbol("=")),
      new PWordAny(5),
      new PToken(new TSymbol("*")),
      new PDoubleAny(),
      new PWordAny(4),
      new PToken(new TSymbol("?")),
      new PToken(new TWord("mpq")),
      new PWordAny(10)
    )
    ptn.naming()

    val regexv = new GenRegexVisitor
    ptn.visit(regexv)
    assertEquals("^(\\d{5})\\+(\\d+)=(\\w{5})\\*(\\d+\\.?\\d*)(\\w{4})\\?mpq(\\w{10})$", regexv.get)

    assertArrayEquals(Array[AnyRef]("_0_0", "_0_2", "_0_4", "_0_6", "_0_7", "_0_10"), regexv.list.toArray[AnyRef])
  }
}
