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

package edu.uchicago.cs.encsel.ptnmining

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.MineColumn._
import edu.uchicago.cs.encsel.ptnmining.analysis.StatUtils
import edu.uchicago.cs.encsel.ptnmining.matching.{GenRegexVisitor, RegexMatcher}
import edu.uchicago.cs.encsel.ptnmining.persist.JPAPatternPersistence
import edu.uchicago.cs.encsel.util.FileUtils

import scala.collection.JavaConverters._

/**
  * This tool is used to mine from columns that were missing before
  * Constant: 19535 is the largest id for original columns
  */
object MineFixer extends App {

  val MAX_ID = 19535
  val persist = new JPAPersistence

  mineAllError

  def mineAllError: Unit = {
    val start = args.length match {
      case 0 => 0
      case _ => args(0).toInt
    }

    val loadcols = persist.em.createQuery("SELECT p FROM Column p WHERE p.dataType = :dt AND EXISTS (SELECT c FROM Column c WHERE c.parentWrapper = p)", classOf[ColumnWrapper])
      .setParameter("dt", DataType.STRING).getResultList

    loadcols.asScala.foreach(column => {

      // Persist pattern
      val pattern = MineColumn.patternFromFile(column.colFile)
      JPAPatternPersistence.save(column, RegexMatcher.genRegex(pattern))
      // Check the unmatched file size, if non-zero, perform a rematch
      val children = getChildren(column)
      if (children.nonEmpty) {
        val unmatch = children.find(_.colIndex == -1)

        unmatch match {
          case None => println("[Error] column %d has no unmatch".format(column.id))
          case Some(umcol) => {
            val numUnmatch = StatUtils.numLine(umcol)
            if (numUnmatch != 0) {
              println("[Info ] column %d has error %d, regenerating".format(column.id, numUnmatch))
              removeChildren(children)
              val splitResult = MineColumn.split(column, pattern)
              persist.save(splitResult)
            }
          }
        }
      }
    })
  }

  def getChildren(col: Column): Seq[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent"
    persist.em.createQuery(sql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }

  def removeChildren(cols: Iterable[Column]): Unit = {
    persist.em.getTransaction.begin()
    cols.foreach(c => {
      new File(c.colFile).delete()
      persist.em.remove(c)
    })
    persist.em.getTransaction.commit()
  }
}
