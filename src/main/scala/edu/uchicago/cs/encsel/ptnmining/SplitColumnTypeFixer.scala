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

import java.io.{FileOutputStream, PrintWriter}
import java.net.URI

import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * This tool fix the column type that is incorrectly marked as string
  */
object SplitColumnTypeFixer extends App {

  val persist = new JPAPersistence
  val patternMiner = new PatternMiner
  val output = new PrintWriter(new FileOutputStream("sql_log"))
  val parentCols = persist.em.createQuery(
    "SELECT c FROM Column c WHERE c.parentWrapper IS NULL",
    classOf[ColumnWrapper]).getResultList

  parentCols.asScala.foreach(col => {
    // Infer the pattern
    val pattern = patternFromFile(col.colFile)

    if (MineColumn.numChildren(pattern) > 0) {
      val seq = pattern.asInstanceOf[PSeq]
      val colPatterns = seq.content.flatMap(_ match {
        case union: PUnion => Some(union)
        case any: PAny => Some(any)
        case _ => None
      }).toList
      val newtype = colPatterns.map(MineColumn.typeof)
      val oldtype = colPatterns.map(oldTypeof)

      oldtype.indices.foreach(i => {
        if (oldtype(i) != newtype(i)) {
          // Mismatch Discovered
          // Get the column and update its type
          //          println("%s:%s:%s:%s".format(col.id, i, oldtype(i), newtype(i)))
          val sql = "UPDATE col_data SET data_type = '%s' WHERE parent_id = %d AND idx = %d;".format(newtype(i).name(), col.id, i)
          output.println(sql)
        }
      })
    }
  })
  output.close

  def oldTypeof(pattern: Pattern): DataType = {
    pattern match {
      case iany: PIntAny => {
        if (iany.maxLength < 0 || (iany.hasHex && iany.maxLength > 8) || (!iany.hasHex && iany.maxLength > 9))
          DataType.LONG
        else
          DataType.INTEGER
      }
      case dany: PDoubleAny => DataType.DOUBLE
      case union: PUnion => {
        DataType.STRING
      }
      case _ => DataType.STRING
    }
  }

  def patternFromFile(file: URI): Pattern = {
    val lines = Source.fromFile(file).getLines().filter(!_.trim.isEmpty).toIterable
    val head = lines.take(500)
    //    val tail = lines.takeRight(100)
    //    val both = head ++ tail
    val pattern = patternMiner.mine(head.map(Tokenizer.tokenize(_).toSeq).toSeq)
    pattern.naming()

    pattern
  }

}
