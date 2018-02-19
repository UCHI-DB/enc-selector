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
  * Created by harper on 3/27/17.
  */
trait RewriteRule {

  private var modified: Boolean = false

  protected def happen() = modified = true

  def happened = modified

  def reset = modified = false

  /**
    * Apply the rule on pattern
    *
    * @param ptn The target Pattern
    * @return pattern after rewritten
    */
  def rewrite(ptn: Pattern): Pattern = modify(ptn, condition, update).get

  /**
    * Utilities Function that rewrite a pattern matching given condition
    *
    * @param root      pattern to start search
    * @param condition condition
    * @param update    function to rewrite the pattern
    * @return the rewritten pattern
    */
  def modify(root: Pattern, condition: Pattern => Boolean, update: Pattern => Pattern): Option[Pattern] = {
    root match {
      case union: PUnion => Some(condition(union) match {
        case true => update(union)
        case false => {
          // Look for match in the union, replace it with rewritten if any
          val modified_content = union.content.map(p => modify(p, condition, update).get)
          val modified_union = modified match {
            case true => new PUnion(modified_content)
            case false => union
          }
          modified_union
        }
      })
      case seq: PSeq => {
        Some(condition(seq) match {
          case true => update(seq)
          case false => {
            val modified_content = seq.content.map(p => modify(p, condition, update).get)
            val modified_seq = modified match {
              case true => new PSeq(modified_content)
              case false => seq
            }
            modified_seq
          }
        })
      }
      case pattern => {
        Some(condition(pattern) match {
          case true => update(pattern)
          case false => pattern
        })
      }
    }
  }

  protected def condition(ptn: Pattern): Boolean

  protected def update(ptn: Pattern): Pattern
}
