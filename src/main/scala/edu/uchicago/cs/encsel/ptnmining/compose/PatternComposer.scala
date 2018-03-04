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

class PatternComposer(pattern: String) {

  val (format, numGroup) = parse(pattern)

  protected def parse(pattern: String): (String, Int) = {
    val buffer = new StringBuilder

    var layer = 0
    var counter = 0
    pattern.foreach(
      _ match {
        case '(' => {
          layer += 1
        }
        case ')' => {
          layer -= 1
          if (layer == 0) {
            buffer.append("%s")
            counter += 1
          }
        }
        case c => {
          if (layer == 0)
            buffer.append(c)
        }
      }
    )

    (buffer.toString, counter)
  }

  def compose(data: Seq[String]): String = {
    if (data.length != numGroup)
      throw new IllegalArgumentException("Expecting %d columns, receiving %d columns".format(numGroup, data.length))
    format.format(data: _*)
  }
}
