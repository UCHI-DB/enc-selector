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
import edu.uchicago.cs.encsel.ptnmining.parser.TWord
import edu.uchicago.cs.encsel.wordvec.SimilarWord

import scala.collection.mutable.ArrayBuffer

object CommonSeqEqualFunc {

  def exactEquals(a: Pattern, b: Pattern): Boolean = a == b

  def patternFuzzyEquals(a: Pattern, b: Pattern): Boolean = {
    (a, b) match {
      case (atk: PToken, btk: PToken) => atk.token.getClass == btk.token.getClass
      case _ => a.equals(b)
    }
  }

  def similarWordEquals(similarWord: SimilarWord): (Pattern, Pattern) => Boolean = {
    val func = (a: Pattern, b: Pattern) =>
      (a, b) match {
        case (atk: PToken, btk: PToken) => {
          (atk.token, btk.token) match {
            case (aw: TWord, bw: TWord) => similarWord.similar(aw.value, bw.value)
            case (at, bt) => at.getClass == bt.getClass
          }
        }
        case _ => a.equals(b)
      }
    func
  }
}

/**
  * Look for common sequence from a union and split it into smaller pieces
  *
  */
class CommonSeqRule(val eqfunc: (Pattern, Pattern) => Boolean = CommonSeqEqualFunc.exactEquals _)
  extends RewriteRule {

  val cseq = new CommonSeq()

  protected def condition(ptn: Pattern): Boolean =
    ptn.isInstanceOf[PUnion] && ptn.asInstanceOf[PUnion].content.size > 1

  protected def update(ptn: Pattern): Pattern = {
    // flatten the union content

    val union = ptn.asInstanceOf[PUnion]
    var hasEmpty = false
    val unionData = union.content.flatMap(
      _ match {
        case seq: PSeq => Some(seq.content)
        case PEmpty => {
          hasEmpty = true
          None
        }
        case p => Some(Seq(p))
      })
    // Look for common sequence
    val seq = cseq.find(unionData, eqfunc)

    if (seq.nonEmpty) {
      happen()

      val sectionBuffers = Array.fill(2 * seq.length + 1)(new ArrayBuffer[Pattern])
      val commonPos = cseq.positions
      val n = seq.length
      commonPos.indices.foreach(j => {
        val pos = commonPos(j)
        val data = unionData(j)

        var pointer = 0

        pos.indices.foreach(i => {
          val sec = pos(i)
          sectionBuffers(2 * i) += PSeq.make(data.view(pointer, sec._1))
          pointer = sec._1 + sec._2
          sectionBuffers(2 * i + 1) += PSeq.make(data.view(sec._1, pointer))
        })
        sectionBuffers.last += (pointer match {
          case last if last == data.length => PEmpty
          case _ => PSeq.make(data.view(pointer, data.length))
        })
      })
      // Create new pattern

      val result = PSeq.make(sectionBuffers.map(s => PUnion.make(s)))
      hasEmpty match {
        case true => PUnion.make(Seq(result, PEmpty))
        case false => result
      }
    } else
      union
  }
}

