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

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}

import scala.collection.mutable.ArrayBuffer

object HexNumber {

  def pattern = "^[0-9a-fA-F]+$".r

  def isNonHexLetter(char: Char) =
    Character.isLetter(char) &&
      ((char > 'f' && char < 'z') || (char > 'F' && char < 'Z'))

  def isHexDigit(char: Char) =
    Character.isDigit(char) ||
      (char >= 'a' && char <= 'f') ||
      (char >= 'A' && char <= 'F')

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
      // Contains at least one seq (a union contains only tokens can be processed by UseAny, no need here)
      // Seq contains only token (at most 2 layer, for simplicity)
      // all content has same length
      val content = union.content.view.filter(_ != PEmpty)
      content.size > 1 && {
        val res = content.map(_ match {
          case seq: PSeq => {
            val sc = seq.content.view
            (sc.forall(_.isInstanceOf[PToken]), true)
          }
          case token: PToken => {
            (token.token.isInstanceOf[TInt] || token.token.isInstanceOf[TWord], false)
          }
          case _ => {
            (false, false)
          }
        })
        res.forall(_._1) && res.exists(_._2)
      }
    }
  }

  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]
    val hasEmpty = union.content.contains(PEmpty)
    // Group the tokens by length
    val grouped = union.content.filter(_ != PEmpty).map(p => (p, p.numChar)).groupBy(_._2).map(_._2.map(_._1))

    val unionedGroup = grouped.flatMap(p => unify(p) match {
      case Some(unified) => Seq(unified)
      case None => p
    }).toSeq ++ (if (hasEmpty) Seq(PEmpty) else Seq())

    if (happened) {
      PUnion(unionedGroup)
    } else {
      ptn
    }
  }

  def unify(input: Seq[Pattern]): Option[Pattern] = {
    if (input.size == 0)
      return None
    if (input.size == 1)
      return Some(input.head)
    // Scan the chars one by one
    val rawData = input.view.map(_.flatten.map(_.asInstanceOf[PToken].token.value).mkString)
    val length = rawData.head.length

    val stopPoint = new ArrayBuffer[Int]
    stopPoint += 0
    // false is number, true is word
    val wordToken = new ArrayBuffer[Boolean]
    val firstLine = rawData.map(_.head)

    var foundException = false
    var foundDigit = false
    var foundNonHex = false
    firstLine.takeWhile(c => {
      foundDigit ||= Character.isDigit(c)
      foundNonHex ||= HexNumber.isNonHexLetter(c)
      foundException = foundDigit && foundNonHex
      !foundException
    }).force

    if (!foundException) {
      var numMode = firstLine.forall(HexNumber.isHexDigit)
      wordToken += numMode

      // If a vertical line contains both digit and non-hex letter, it is considered invalid
      // and no conversion is performed
      (1 until length).takeWhile(i => {
        val chars = rawData.map(_.charAt(i))
        foundException = chars.exists(Character.isDigit) && chars.exists(HexNumber.isNonHexLetter)
        foundException match {
          case false => {
            (numMode ^ chars.forall(HexNumber.isHexDigit)) match {
              case false => {
                stopPoint.update(stopPoint.length - 1, i)
              }
              case true => {
                stopPoint += i
                numMode = !numMode
                wordToken += numMode
              }
            }
          }
          case _ => {}
        }
        !foundException
      })
    }
    if (foundException) {
      None
    } else {
      happen()
      val to = stopPoint.view.map(_ + 1)
      val from = 0 +: to.dropRight(1)
      val ranges = from.zip(to).zip(wordToken)
      // map the partition to original data to rebuild token
      val tokens = rawData.map(line => {
        ranges.map(r => {
          val value = line.substring(r._1._1, r._1._2)
          new PToken(r._2 match {
            case true => new TInt(value)
            case false => new TWord(value)
          })
        })
      })
      Some(PSeq(ranges.indices.map(i => PUnion(tokens.map(_ (i))))))
    }
  }

}
