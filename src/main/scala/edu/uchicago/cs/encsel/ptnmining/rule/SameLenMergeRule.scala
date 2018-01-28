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

import edu.uchicago.cs.encsel.ptnmining.parser.TInt
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion, Pattern}

object HexNumber {

  def pattern = "^[0-9a-fA-F]+$".r

  def isHex(input: String) = pattern.findFirstMatchIn(input).isDefined
}

/**
  * This rule looks at unions having the same length of sequences, and tries to apply some conversion to them
  */
class SameLenMergeRule extends RewriteRule {

  override protected def condition(ptn: Pattern): Boolean = {
    ptn.isInstanceOf[PUnion] && {
      val union = ptn.asInstanceOf[PUnion]

      // Contains only seq or token
      // Seq contains only token (at most 2 layer, this is for simplicity)
      // all content has same length
      val content = union.content.view
      content.forall(p => (p.isInstanceOf[PSeq] || p.isInstanceOf[PToken])) &&
        content.filter(_.isInstanceOf[PSeq])
          .forall(_.asInstanceOf[PSeq].content.forall(_.isInstanceOf[PToken])) &&
        content.map(_.numChar).filter(_ != 0).toSet.size == 1
    }
  }

  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]
    // Hex check
    val mapped = union.content.map(_ match {
      case token: PToken => token
      case seq: PSeq => {
        val view = seq.content.view
        val tkvals = view.map(_.asInstanceOf[PToken].token.value)
        if (tkvals.forall(HexNumber.isHex)) {
          happen()
          new PToken(new TInt(tkvals.mkString))
        }
        else
          seq
      }
    })
    happened match {
      case true => PUnion(mapped)
      case false => union
    }
  }
}
