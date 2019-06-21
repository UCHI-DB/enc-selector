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

import edu.uchicago.cs.encsel.dataset.feature.compress.ParquetCompressFileSize
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType

import scala.collection.JavaConverters._

object EncodeAllSubColumn extends App {

  val persist = new JPAPersistence
  val columns = persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper IS NOT NULL AND c.dataType = :dt",
    classOf[ColumnWrapper]).setParameter("dt", DataType.STRING).getResultList.asScala
  columns.foreach(colw => {
    println(colw.id)
//    colw.replaceFeatures(ParquetEncFileSize.extract(colw))
    colw.replaceFeatures(ParquetCompressFileSize.extract(colw))
    persist.save(Seq(colw))
  })
}

object EncodeSingleSubColumn extends App {
  val persist = new JPAPersistence
  val column = persist.find(args(0).toInt)
  column.replaceFeatures(ParquetEncFileSize.extract(column))
  persist.save(Seq(column))
}
