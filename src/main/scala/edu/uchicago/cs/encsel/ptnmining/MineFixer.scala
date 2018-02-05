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

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.MineColumn._
import edu.uchicago.cs.encsel.ptnmining.matching.GenRegexVisitor

import scala.collection.JavaConverters._

/**
  * This tool is used to mine from columns that were missing before
  * Constant: 19535 is the largest id for original columns
  */
object MineFixer extends App {

  val MAX_ID = 19535
  val persist = new JPAPersistence

  mineAllMissing

  def mineAllMissing: Unit = {
    val start = args.length match {
      case 0 => 0
      case _ => args(0).toInt
    }


    val loadcols = persist.em.createQuery("SELECT p FROM Column p WHERE p.dataType = :dt AND p.id <= :id", classOf[ColumnWrapper])
      .setParameter("dt", DataType.STRING).setParameter("id", MAX_ID).getResultList

    loadcols.asScala.foreach(col => {
      val column = col.asInstanceOf[ColumnWrapper]
      val colid = column.id
      val pattern = patternFromFile(column.colFile)
      val valid = numChildren(pattern)
      val children = getChild(column)
      if (valid != children.size) {
        val regex = new GenRegexVisitor
        pattern.visit(regex)
        println("%d:%s:%s:%s".format(colid, valid, children.size, regex.get))
        //        val subcols = SplitColumn.split(column, pattern)
        //        if (!subcols.isEmpty)
        //          persist.save(subcols)
      }
    })
  }

  def getChild(col: Column): Seq[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent"
    persist.em.createQuery(sql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }

  def removeChild(col: Column): Unit = {

  }
}
