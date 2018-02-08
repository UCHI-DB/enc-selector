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

package edu.uchicago.cs.encsel.ptnmining.analysis

import java.io.{FileOutputStream, PrintWriter}

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.analysis.GetEncodeBenefit.persist

import scala.collection.JavaConverters._

object GetUnmatchRate extends App {

  val unmatchRecord = new PrintWriter(new FileOutputStream("high_unmatch"))

  val persist = new JPAPersistence
  val columns = persist.em.createQuery("SELECT c FROM Column c WHERE c.dataType = :dt AND EXISTS (SELECT ch FROM Column ch WHERE ch.parentWrapper = c)", classOf[ColumnWrapper]).setParameter("dt", DataType.STRING).getResultList.asScala

  columns.foreach(col => {
    val originalSize = StatUtils.numLine(col)
    val unmatchSize = StatUtils.numLine(getUnmatchChild(col))
    val ratio = unmatchSize.toDouble / originalSize
    if (ratio > 0.3) {
      unmatchRecord.println(col.id, ratio)
    }
  })

  unmatchRecord.close

  def getUnmatchChild(col: Column): Column = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent AND c.colIndex = -1"
    try {
      persist.em.createQuery(sql, classOf[ColumnWrapper]).setParameter("parent", col).getSingleResult
    } catch {
      case e: Exception => {
        null
      }
    }
  }
}
