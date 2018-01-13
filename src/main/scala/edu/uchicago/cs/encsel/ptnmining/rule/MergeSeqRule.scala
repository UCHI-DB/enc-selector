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

import edu.uchicago.cs.encsel.ptnmining.{PSeq, Pattern}

/**
  * This rule merges unnecessary PSeq.
  * E.g.,
  *   Seq(Seq(a,b),Seq(x,y)) => Seq(a,b,x,y)
  *   Seq(Seq(a,b),x,y) => Seq(a,b,x,y)
  */
class MergeSeqRule extends RewriteRule {

  protected def condition(ptn: Pattern): Boolean = ptn.isInstanceOf[PSeq]

  protected def update(ptn: Pattern): Pattern = {
    val seq = ptn.asInstanceOf[PSeq]
    val check = seq.content.map(_.isInstanceOf[PSeq]).reduce((a, b) => a || b)
    check match {
      case true => {
        happen()
        PSeq.make(seq.content.flatMap(_ match {
          case seq: PSeq => seq.content
          case x => Seq(x)
        }))
      }
      case false => ptn
    }
  }
}
