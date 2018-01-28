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

package edu.uchicago.cs.encsel.ptnmining.genregex

import edu.uchicago.cs.encsel.ptnmining._

import scala.collection.mutable

/**
  * Generate a regular expression string that contains the given pattern.
  * Some patterns, e.g., <code>PIntRange</code> cannot be represented
  * exactly by regular expressions, in that case, we will use the superset
  * to represent it
  *
  */
class GenRegexVisitor extends PatternVisitor {

  val history = new mutable.HashMap[String, String]

  override def on(ptn: Pattern): Unit = {
    ptn match {
      case token: PToken => {
        history.put(token.name, token.token.value)
      }
      case PEmpty => {

      }
      case wany: PWordAny => {
        history.put(wany.name,
          (wany.minLength, wany.maxLength) match {
            case (1, -1) => "\\w+"
            case (i, j) if i == j => "\\w{%d}".format(i)
            case (i, j) => "\\w{%d,%d}".format(i, j)
          })
      }
      case iany: PIntAny => {
        val digit = iany.hasHex match {
          case true => "\\d"
          case false => "[0-9a-fA-F]"
        }
        history.put(iany.name,
          (iany.minLength, iany.maxLength) match {
            case (1, -1) => "%s+".format(digit)
            case (i, j) if i == j => "%s{%d}".format(digit, i)
            case (i, j) => "%s{%d,%d}".format(digit, i, j)
          })
      }
      case dany: PDoubleAny => {
        history.put(dany.name, "\\d+\\.\\d+")
      }
      case irng: PIntRange => {
        // Use PIntAny instead
        history.put(irng.name, "\\d+")
      }
      case _ => {}
    }
  }

  override def exit(ptn: Pattern): Unit = {
    super.exit(ptn)
    ptn match {
      case union: PUnion => {
        var result = union.content.filter(_ != PEmpty).map(n => history.getOrElse(n.name, "<err>"))
          .reduce((a, b) => "(%s)|(%s)".format(a, b))
        if (union.content.contains(PEmpty)) {
          result = "(%s)?".format(result)
        }
        union.content.foreach(n => history.remove(n.name))
        history.put(union.name, result)
      }
      case seq: PSeq => {
        val res = seq.content.filter(_ != PEmpty)
          .map(n => history.getOrElse(n.name, "<err>")).mkString
        seq.content.foreach(n => history.remove(n.name))
        history.put(seq.name, res)
      }
      case _ => {}
    }
  }
}
