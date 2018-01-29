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

package edu.uchicago.cs.encsel.ptnmining

import java.io.{File, FileOutputStream, PrintWriter}

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.util.FileUtils
import org.apache.commons.lang3.StringUtils

import scala.io.Source

/**
  * Split a column to sub-columns using a given pattern
  */
object ColumnSplit extends App {

  val persistence = new JPAPersistence

  def splitColumn(column: Column, pattern: Pattern): Seq[Column] = {
    pattern.naming()
    pattern match {
      case seq: PSeq => {
        val numColumns = seq.content.map(_ match {
          case token: PToken => 0
          case _ => 1
        }).sum
        val childColumns = (0 until numColumns).map(i => {
          val col = new Column()
          col.colIndex = i
          col.colName = String.valueOf(i)
          col.colFile = FileUtils.addExtension(column.colFile, i.toString)
          col.parent = column
          col.dataType = DataType.INTEGER
          col
        })

        val outputs = childColumns.map(col => new PrintWriter(new FileOutputStream(new File(col.colFile))))

        Source.fromFile(column.colFile).getLines().foreach(line => {
          StringUtils.isEmpty(line) match {
            case false => {
              val split = line.split("-")
              (0 until numColumns).foreach(_ match {
                case sm if sm < split.length => {
                  val value =
                    try {
                      Integer.parseInt(split(sm + 1))
                    } catch {
                      case e: NumberFormatException => {
                        Integer.parseInt(split(sm + 1), 16)
                      }
                    }
                  outputs(sm).println(value)
                }
                case lg => outputs(lg).println("")
              })
            }
            case true => {
              (0 until numColumns).foreach(i => {
                outputs(i).println("")
              })
            }
          }
        })
        outputs.foreach(_.close)
        childColumns
      }
      case _ => {
        Seq()
      }
    }
  }

}
