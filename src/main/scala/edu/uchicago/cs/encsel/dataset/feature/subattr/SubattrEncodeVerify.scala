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

package edu.uchicago.cs.encsel.dataset.feature.subattr

import java.nio.file.{Files, Paths}

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.Feature
import edu.uchicago.cs.encsel.dataset.feature.subattr.SubattrEncodeSingleFile.{em, patternSql, _}
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.parquet.ParquetTupleReader
import edu.uchicago.cs.encsel.ptnmining.compose.PatternComposer
import edu.uchicago.cs.encsel.ptnmining.persist.PatternWrapper
import edu.uchicago.cs.encsel.util.FileUtils

import scala.collection.JavaConverters._

object SubattrEncodeVerify extends App {
  val em = new JPAPersistence().em
  val sql = "SELECT c FROM Column c WHERE EXISTS (SELECT p FROM Column p WHERE p.parentWrapper = c) ORDER BY c.id"
  val childSql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent"
  val patternSql = "SELECT p FROM Pattern p WHERE p.column = :col"

  em.createQuery(sql, classOf[ColumnWrapper]).getResultList.asScala.foreach(col => {
    println("Processing column %d".format(col.id))
    val children = getChildren(col)

    // Build a single table
    val validChildren = children.filter(_.colName != "unmatch")
    val pattern = new PatternComposer(getPattern(col).pattern)

    if (pattern.numGroup != validChildren.size)
      println("%d parsed pattern is inconsistent with extracted columns")

    val subtable = FileUtils.addExtension(col.colFile, "subtable")
    if (Files.exists(Paths.get(subtable))) {
      val parquetReader = new ParquetTupleReader(subtable)

      if (validChildren.size != 1) {
        println("%d has different children count".format(col.id))
      }
      if (parquetReader.getNumOfRecords != validChildren.head) {
        println("%d subtable count is different from children count".format(col.id))
      }
    }
  })

  def getChildren(col: Column): Seq[Column] = {
    em.createQuery(childSql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }

  def parquetLineCount(col: Column): Long = {
    FileUtils.numLine(col.colFile)
  }

  def getPattern(col: Column) = {
    em.createQuery(patternSql, classOf[PatternWrapper]).setParameter("col", col).getSingleResult
  }
}
