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

package edu.uchicago.cs.encsel.ptnmining.compose

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RegexParser {

  def parse(input: String): Seq[Token] = {
    val buffer = new ArrayBuffer[Token]
    if (input.isEmpty)
      buffer
    else {
      var escape = false
      var layer = 0
      var groupBuffer = new ArrayBuffer[GroupToken]
      var current: Token = null
      input.foreach(_ match {
        case esc if escape => {
          current = new SimpleToken(esc, 0, true)
          if (layer == 0)
            buffer += current
          else
            groupBuffer.last.children += current
          escape = false
        }
        case '\\' => {
          escape = true
        }
        case '(' => {
          groupBuffer += new GroupToken
          layer += 1
        }
        case ')' => {
          buffer += groupBuffer.remove(groupBuffer.length - 1)
          current = buffer.last
          layer -= 1
        }
        case '?' => {
          current.rep = 1
        }
        case '+' => {
          current.rep = 3
        }
        case '*' => {
          current.rep = 2
        }
        case c => {
          current = new SimpleToken(c)
          if (layer == 0) {
            buffer += current
          } else {
            groupBuffer.last.children += current
          }
        }
      })
      buffer
    }
  }

}

/*
* @param rep 0 for normal, 1 for ?, 2 for *, 3 for +
*/
object Token {
  val SYMBOL = Array("%s", "%s?", "%s*", "%s+")
}

trait Token {
  var rep: Int = _
}

class GroupToken(r: Int = 0) extends Token {
  rep = r
  val children: mutable.Buffer[Token] = new ArrayBuffer[Token]

  override def toString: String = {
    Token.SYMBOL(rep).format("(%s)".format(children.map(_.toString).mkString("")))
  }
}

/**
  *
  * @param content
  */
class SimpleToken(var content: Char, r: Int = 0, var escape: Boolean = false) extends Token {
  rep = r

  override def toString: String = {
    var result = Token.SYMBOL(rep).format(content)
    if (escape)
      result = "\\%s".format(result)
    return result
  }
}