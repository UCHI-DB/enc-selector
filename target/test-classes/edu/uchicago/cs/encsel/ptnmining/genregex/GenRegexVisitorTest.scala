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

package edu.uchicago.cs.encsel.ptnmining.genregex

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining._
import org.junit.Test
import org.junit.Assert._

class GenRegexVisitorTest {

  @Test
  def testVisit: Unit = {
    val ptn = new PSeq(
      new PIntAny(5),
      new PToken(new TWord("And")),
      new PIntAny(),
      PEmpty,
      new PUnion(
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
    assertEquals("\\d{5}And\\d+(((dmd)|(12312))|(\\d{3,4}))?\\w{5}3077\\d+\\.\\d+\\w+mpq\\w{10}",
      regexv.history.getOrElse(ptn.name, "-1"))
  }
}
