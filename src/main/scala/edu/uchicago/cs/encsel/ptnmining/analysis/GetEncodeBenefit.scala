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

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType

import scala.collection.JavaConverters._


object GetEncodeBenefit extends App {

  val persist = new JPAPersistence
  val columns = persist.em.createQuery("SELECT c FROM Column c WHERE c.dataType = :dt AND EXISTS (SELECT ch FROM Column ch WHERE ch.parentWrapper = c)", classOf[ColumnWrapper]).setParameter("dt", DataType.STRING).getResultList.asScala
  columns.foreach(col => {
    val originalSize = columnSize(col)
    val childrenSize = getChildren(col).map(columnSize).sum
    if (originalSize > 0 && childrenSize > 0) {
      val ratio = childrenSize / originalSize;
      if (ratio != 0) {
        col.infos.put("subattr_benefit", ratio)
        persist.save(Seq(col))
      }
    }
  })

  def getChildren(col: Column): Seq[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent"
    persist.em.createQuery(sql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }

  def columnSize(col: Column): Double = {
    val features = col.findFeatures(ParquetEncFileSize.featureType).map(_.value).filter(_ > 0)
    features.size match {
      case 0 => 0
      case _ => features.min
    }
  }
}
