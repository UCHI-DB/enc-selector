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

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PSeq, PToken, PUnion, Pattern}

import scala.collection.mutable.ArrayBuffer

object HexNumber {

  def pattern = "^[0-9a-fA-F]+$".r

  def isHex(input: String) = pattern.findFirstMatchIn(input).isDefined
}

/**
  * This rule looks at unions having the same length of sequences, and tries to organize them to groups
  * During this process transformation may be applied , e.g. converting character to hex number
  */
class SameLenMergeRule extends RewriteRule {

  override protected def condition(ptn: Pattern): Boolean = {
    ptn.isInstanceOf[PUnion] && {
      val union = ptn.asInstanceOf[PUnion]
      // Contains only seq or token
      // Seq contains only token (at most 2 layer, this is for simplicity)
      // all content has same length
      val content = union.content.view
      content.size > 1 && {
        val res = content.map(_ match {
          case seq: PSeq => {
            val sc = seq.content.view
            (sc.forall(_.isInstanceOf[PToken]), sc.map(_.numChar).sum)
          }
          case token: PToken => {
            (token.token.isInstanceOf[TInt] || token.token.isInstanceOf[TWord], token.numChar)
          }
          case _ => {
            (false, -1)
          }
        })
        res.forall(_._1) && res.map(_._2).toSet.size == 1
      }
    }
  }

  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]

    // Scan the chars one by one
    val rawData = union.content.view.map(_.flatten.map(_.asInstanceOf[PToken].token.value).mkString)
    val length = rawData.head.length

    val startPoint = new ArrayBuffer[Int]
    val stopPoint = new ArrayBuffer[Int]
    startPoint += 0
    stopPoint += 1
    // false is number, true is word
    val wordToken = new ArrayBuffer[Boolean]
    wordToken += false
    var numMode = true

    for (i <- 0 until length) {
      val chars = rawData.map(_.charAt(i).toString)
      (numMode ^ chars.forall(HexNumber.isHex)) match {
        case false => {
          stopPoint.last += 1
        }
        case true => {
          startPoint += stopPoint.last + 1
          stopPoint += startPoint.last + 1
          wordToken += numMode
          numMode = !numMode
        }
      }
    }
    val ranges = startPoint.zip(stopPoint).zip(wordToken)
    // map the partition to original data to rebuild token
    val tokens = rawData.map(line => {
      val start = 0
      ranges.map(r => {
        val value = line.substring(r._1._1, r._1._2)
        new PToken(r._2 match {
          case true => new TWord(value)
          case false => new TInt(value)
        })
      })
    })
    PSeq(ranges.indices.map(i => PUnion(tokens.map(_(i)))).toSeq)
  }
}
