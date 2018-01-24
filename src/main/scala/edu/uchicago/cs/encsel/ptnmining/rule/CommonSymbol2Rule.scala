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
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol

/**
  * Allows the pattern without symbols to be accepted and categorized to the first union
  */
class CommonSymbol2Rule extends RewriteRule {
  protected def condition(ptn: Pattern): Boolean =
    ptn.isInstanceOf[PUnion] && ptn.asInstanceOf[PUnion].content.size > 1

  protected def update(union: Pattern): Pattern = {
    // flatten the union content
    val unionData = union.asInstanceOf[PUnion].content
      .map(_ match { case seq: PSeq => seq.content case p => Seq(p) })
    // Scan union data for symbols
    val symbolsWithIndex = unionData.map(_.zipWithIndex.filter(_._1 match {
      case t: PToken => t.token.isInstanceOf[TSymbol]
      case _ => false
    })).zipWithIndex
    // Determine common symbols and match symbol in each line to the common

    val indicesWithNoSymbol = symbolsWithIndex.filter(_._1.isEmpty).map(p => (p._2, p._1.map(_._1))).toMap
    val dataWithSymbol = symbolsWithIndex.filter(!_._1.isEmpty)
    val indicesWithSymbol = dataWithSymbol.zipWithIndex.map(p => (p._1._2, (p._1._1, p._2))).toMap
    val symbolsWithSymbol = dataWithSymbol.map(_._1)

    val commonSeq = new CommonSeq
    val commonSymbols = commonSeq.find(symbolsWithSymbol, (a: (Pattern, Int), b: (Pattern, Int)) => {
      a._1.equals(b._1)
    }).flatten
    val n = commonSymbols.length
    commonSymbols.isEmpty match {
      case true => union
      case false => {
        happen()
        // Use the positions to split data and generate new unions

        // n common symbols split the data to at most n+1 pieces
        val pieces = (0 to n).map(i => {
          PUnion.make(unionData.indices.map(j => {
            if (indicesWithNoSymbol.contains(j)) {
              i match {
                case 0 => PSeq.make(indicesWithNoSymbol.getOrElse(j, Seq()))
                case _ => PEmpty
              }
            } else if (indicesWithSymbol.contains(j)) {
              val data = unionData(j)
              val symbols = indicesWithSymbol.get(j).get
              val pos = commonSeq.positions(symbols._2)
              val index = pos.map(p => symbols._1.view(p._1, p._1 + p._2)).flatten.map(_._2)

              val start = i match {
                case 0 => 0
                case _ => index(i - 1) + 1
              }
              val stop = i match {
                case last if last == n => data.length
                case _ => index(i)
              }
              stop - start match {
                case 0 => PEmpty
                case 1 => data(start)
                case _ => PSeq.make(data.view(start, stop))
              }
            } else {
              PEmpty
            }
          }))
        })
        // Make a sequence formed of union pieces and common sequences
        PSeq.make((0 until 2 * n + 1).view.map(i => {
          i % 2 match {
            case 0 => pieces(i / 2)
            case 1 => commonSymbols((i - 1) / 2)._1
          }
        }).filter(_ match {
          case union: PUnion => union.content.nonEmpty
          case _ => true
        }))
      }
    }
  }
}
