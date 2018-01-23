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

package edu.uchicago.cs.encsel.ptnmining

import edu.uchicago.cs.encsel.ptnmining.parser.Token
import edu.uchicago.cs.encsel.ptnmining.rule._

/**
  * Created by harper on 3/25/17.
  */
class PatternMiner {

  val rules = Array(new CommonSymbolRule, new CommonSeqRule,
    new SuccinctRule, new MergeSeqRule, new IntegerRangeRule, new UseAnyRule)

  def mine(in: Seq[Seq[Token]]): Pattern = {
    // Generate a direct pattern by translating tokens

    val translated = new PUnion(in.map(l => new PSeq(l.map(new PToken(_)): _*)))

    rules.foreach(rule => {
      rule match {
        case data: DataRewriteRule => data.generateOn(in)
        case _ => Unit
      }
    })
    // Repeatedly refine the pattern using supplied rules
    var toRefine: Pattern = translated
    var needRefine = true
    var refineResult: Pattern = toRefine
    while (needRefine) {
      val refined = refine(toRefine)
      if (refined._2) {
        toRefine = refined._1
      } else {
        needRefine = false
        refineResult = refined._1
      }
    }

    val validated = validate(refineResult)
    validated
  }

  protected def refine(root: Pattern): (Pattern, Boolean) = {
    var current = root

    rules.indices.foreach(i => {
      rules(i).reset
      current = rules(i).rewrite(current)
      if (rules(i).happened) {
        // Apply the first valid rule
        return (current, true)
      }
    })
    (root, false)
  }

  def validate(ptn: Pattern): Pattern = {
    ptn
  }

}

