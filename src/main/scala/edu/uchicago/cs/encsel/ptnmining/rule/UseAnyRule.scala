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
 *
 */

package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TDouble, TInt, TWord}

/**
  * If the union size is too large, e.g., 90% of total size
  * Use Any to replace big Union
  */
object UseAnyRule {
  // Execute the rule if union size is greater than threshold * data size
  val threshold = 0.1
}

class UseAnyRule extends DataRewriteRule {

  override def condition(ptn: Pattern): Boolean =
    ptn.isInstanceOf[PUnion] && ptn.asInstanceOf[PUnion].content.length >= UseAnyRule.threshold * originData.length


  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]
    toAny(union)
  }

  protected def toAny(ptn: Pattern): Pattern = {
    ptn match {
      case union: PUnion => PUnion.make(union.content.map(toAny))
      case seq: PSeq => PSeq.make(seq.content.map(toAny))
      case token: PToken => {
        token.token match {
          case word: TWord => new PWordAny
          case int: TInt => new PIntAny
          case double: TDouble => new PDoubleAny
          case _ => token
        }
      }
      case _ => ptn
    }
  }

  def fuzzyCompare(a: Pattern, b: Pattern): Boolean = {
    (a, b) match {
      case (at: PToken, bt: PToken) => {
        at.token.isData match {
          case true => at.token.getClass == bt.token.getClass
          case false => at.token.equals(bt.token)
        }
      }
      case (_, _) => {
        a.equals(b)
      }
    }
  }
}

