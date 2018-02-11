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
import javax.persistence.NoResultException

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
      // Check the unmatched file size, if non-zero, perform a rematch
      getUnmatch(column) match {
        case Some(unmatch) => {
          val numUnmatch = FileUtils.numLine(unmatch.colFile)
          if (numUnmatch != 0) {
            println("[Info ] column %d has error %d, regenerating".format(column.id, numUnmatch))
            mineColumn(column)
          }
        }
        case _ => {
          println("[Error] column %d has no unmatch".format(column.id))
        }
      }
    })
  }

  def getUnmatch(col: Column): Option[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent AND c.idx = :idx"
    try {
      Some(persist.em.createQuery(sql, classOf[ColumnWrapper])
        .setParameter("parent", col)
        .setParameter("idx", -1)
        .getSingleResult)
    } catch {
      case e: NoResultException => None
    }
  }

}
