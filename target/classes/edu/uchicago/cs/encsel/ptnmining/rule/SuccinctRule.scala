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

/**
  * Reduce unnecessary seq or union structure
  * Seq(a) => a
  * Union(a) => a
  * Seq(a,PEmpty) => Seq(a)
  * Seq() => PEmpty
  * Union() => PEmpty
  */
class SuccinctRule extends RewriteRule {

  protected def condition(p: Pattern): Boolean = {
    p match {
      case u: PUnion => u.content.size <= 1
      case s: PSeq => s.content.length <= 1 || s.content.contains(PEmpty)
      case _ => false
    }
  }

  protected def update(up: Pattern): Pattern = {
    up match {
      case seq: PSeq => {
        happen()
        val removeEmpty = seq.content.filter(_ != PEmpty)
        removeEmpty.length match {
          case 0 => PEmpty
          case 1 => //noinspection ZeroIndexToHead
            removeEmpty(0)
          case _ => new PSeq(removeEmpty)
        }
      }
      case union: PUnion => {
        happen()
        union.content.length match {
          case 0 => PEmpty
          case 1 => //noinspection ZeroIndexToHead
            union.content(0)
        }
      }
      case _ => up
    }
  }
}
