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

package edu.uchicago.cs.encsel.ptnmining.compose

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PatternComposer(pattern: String) {

  private val booleanColIndex = new mutable.HashSet[Int]

  private val optionalColIndex = new mutable.HashSet[Int]

  private val groups = new ArrayBuffer[String]

  val (format, numGroup) = parse(pattern)

  protected def parse(pattern: String): (String, Int) = {
    val buffer = new StringBuilder
    val escapeBuffer = new StringBuilder
    val groupBuffer = new StringBuilder
    var layer = 0
    var counter = 0
    var escape = false
    pattern.foreach(
      _ match {
        case esc if escape => {
          val translated = esc match {
            case 's' => ' '
            case _ => "\\%s".format(esc)
          }
          if (layer > 0) {
            groupBuffer.append(translated)
          } else {
            buffer.append(translated)
          }
          escape = false
        }
        case '\\' => {
          escape = true
        }
        case '(' => {
          if (layer > 0) {
            groupBuffer.append('(')
          }
          layer += 1
        }
        case ')' => {
          layer -= 1
          if (layer == 0) {
            buffer.append("%s")
            counter += 1
            groups += groupBuffer.toString
            groupBuffer.clear()
          } else {
            groupBuffer.append(')')
          }
        }
        case '+' => {
          if (layer > 0) {
            groupBuffer.append('+')
          } else {
            // Ignore
          }
        }
        case '?' => {
          if (layer > 0) {
            groupBuffer.append('?')
          } else if (groups.length > 0 && groupBuffer.length == 0 && isSymbol(groups.last)) {
            // ? occurrs just after group
            optionalColIndex += groups.length - 1
            // Just done with a group
            booleanColIndex += groups.length - 1
          } else {
            // Ignore
          }
        }
        case '^' | '$' => {
          // Ignore
        }
        case c => {
          if (layer == 0)
            buffer.append(c)
          else
            groupBuffer.append(c)
        }
      }
    )
    (buffer.toString, counter)
  }

  protected def isSymbol(str: String): Boolean = {
    str.length == 1 && !Character.isLetterOrDigit(str.head)
  }

  def booleanColumns: Set[Int] = booleanColIndex.toSet

  def optionalColumns: Set[Int] = optionalColIndex.toSet

  def compose(data: Seq[String]): String = {
    if (data.length != numGroup)
      throw new IllegalArgumentException("Expecting %d columns, receiving %d columns".format(numGroup, data.length))

    val dataArray = data.toArray
    booleanColumns.foreach(i => {
      dataArray(i) = if (data(i) == "true") groups(i) else ""
    })

    format.format(dataArray: _*)
  }
}
