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
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol

import scala.collection.mutable

/**
  * Look for common separators (non-alphabetic, non-numerical characters) from Union and use them to
  * split data
  */
class SeparatorRule extends RewriteRule {
  protected def condition(ptn: Pattern): Boolean =
    ptn.isInstanceOf[PUnion] && ptn.asInstanceOf[PUnion].content.size > 1

  protected def update(union: Pattern): Pattern = {
    // flatten the union content
    val unionData = union.asInstanceOf[PUnion].content.map(p => {
      p match {
        case seq: PSeq => seq.content
        case _ => Seq(p)
      }
    })
    // Scan union data for symbols
    val symbols = unionData.map(line => {
      line.filter(p => p match {
        case t: PToken => t.token.isInstanceOf[TSymbol]
        case _ => false
      })
    })
    // Determine common symbols and match symbol in each line to the common
    val symbolList = new mutable.ArrayBuffer[TSymbol]
    val symbolMatch =


    null
  }

}
