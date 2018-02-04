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
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.matching.RegexMatcher
import edu.uchicago.cs.encsel.util.FileUtils
import org.apache.commons.lang3.StringUtils

import scala.io.Source

/**
  * Split a column to sub-columns using a given pattern
  *
  * The following limitations are applied
  *
  * 1. Pattern is a PSeq, as other types cannot be split
  * 2. Only PUnion/PAny elements in PSeq are treated as subcolumns.
  * 3. Data type is determined by the pattern element type.
  * 4. Each line will be matched against the pattern.
  * Lines that cannot be matched will be put to an additional column
  */
object SplitColumn {

  val matcher = RegexMatcher

  def split(column: Column, pattern: Pattern): Seq[Column] = {
    if (column.dataType != DataType.STRING)
      throw new IllegalArgumentException()
    pattern.naming()
    pattern match {
      case seq: PSeq => {
        val colPatterns = seq.content.flatMap(_ match {
          case union: PUnion => Some(union)
          case any: PAny => Some(any)
          case _ => None
        })
        val childColumns = colPatterns.zipWithIndex.map(pi => {
          val col = new Column(null, pi._2, String.valueOf(pi._2), typeof(pi._1))
          col.colFile = FileUtils.addExtension(column.colFile, pi._2.toString)
          col.parent = column
          col
        })
        // columns for unmatched lines
        val unmatchCol = new Column(null, -1, "unmatch", DataType.STRING)
        unmatchCol.colFile = FileUtils.addExtension(column.colFile, "unmatch")
        unmatchCol.parent = column

        val outputs = childColumns.map(col => new PrintWriter(new FileOutputStream(new File(col.colFile))))
        val umoutput = new PrintWriter(new FileOutputStream(new File(unmatchCol.colFile)))

        val source = Source.fromFile(column.colFile)
        try {
          source.getLines().foreach(line => {
            if (!StringUtils.isEmpty(line)) {
              // Match the line against pattern
              val matched = matcher.matchon(pattern, line)
              if (matched.isDefined && colPatterns.forall(p => matched.get.has(p.name))) {
                colPatterns.indices.foreach(i => {
                  val ptn = colPatterns(i)
                  ptn match {
                    case iany: PIntAny => {
                      val radix = if (iany.hasHex) 16 else 10
                      outputs(i).println(BigInt(matched.get.get(ptn.name), radix).toString)
                    }
                    case _ => {
                      outputs(i).println(matched.get.get(ptn.name))
                    }
                  }

                })
              } else {
                // Not match, write to unmatch
                umoutput.println(line)
              }
            } else {
              // Output empty line for empty line
              outputs.foreach(_.println(""))
            }
          })
          childColumns
        } finally {
          source.close
          outputs.foreach(_.close)
          umoutput.close
        }
      }
      case _ => Seq()
    }
  }

  protected def typeof(pattern: Pattern): DataType = {
    pattern match {
      case iany: PIntAny => {
        if (iany.maxLength < 0 || (iany.hasHex && iany.maxLength > 8) || (!iany.hasHex && iany.maxLength > 9))
          DataType.LONG
        else
          DataType.INTEGER
      }
      case dany: PDoubleAny => DataType.DOUBLE
      case _ => DataType.STRING
    }
  }

}
