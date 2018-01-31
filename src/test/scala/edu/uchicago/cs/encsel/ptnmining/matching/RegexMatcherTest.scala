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

import edu.uchicago.cs.encsel.ptnmining.{PIntAny, PSeq, PUnion, PWordAny}
import org.junit.Test
import org.junit.Assert._

class RegexMatcherTest {

  @Test
  def testMatch: Unit = {

    val ptn = PSeq(Seq(
      new PIntAny(4, 4, true),
      new PWordAny(3, 3),
      PUnion(Seq(
        new PIntAny(5),
        new PWordAny(3)
      )),
      new PWordAny(3)
    ))
    ptn.naming()
    val test1 = "1234qmP55237oWD"
    val test2 = "1233KKdMPPwoq"
    val test3 = "12341423owdfwr"

    val match1 = RegexMatcher.matchon(ptn, test1)

    assertTrue(match1.isDefined)
    val m1 = match1.get
    assertEquals("1234", m1.get("_0_0"))
    assertEquals("qmP", m1.get("_0_1"))
    assertEquals("55237", m1.get("_0_2"))
    assertEquals("oWD", m1.get("_0_3"))

    val match2 = RegexMatcher.matchon(ptn, test2)

    assertTrue(match2.isDefined)
    val m2 = match2.get
    assertEquals("1233", m2.get("_0_0"))
    assertEquals("KKd", m2.get("_0_1"))
    assertEquals("MPP", m2.get("_0_2"))
    assertEquals("woq", m2.get("_0_3"))

    val match3 = RegexMatcher.matchon(ptn, test3)

    assertTrue(match3.isEmpty)
  }
}
