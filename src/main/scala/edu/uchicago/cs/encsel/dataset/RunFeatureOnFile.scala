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

package edu.uchicago.cs.encsel.dataset

import java.net.URI

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.Features
import edu.uchicago.cs.encsel.dataset.feature.compress.ParquetCompressTimeUsage
import edu.uchicago.cs.encsel.model.DataType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object RunFeatureOnFile extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val torun = ParquetCompressTimeUsage

  val file = args(0)

  var column = new Column
  column.dataType = DataType.STRING

  column.colFile = new URI(args(0))

  System.out.println("Processing %s".format(column.colFile))
  try {
    column.replaceFeatures(Features.extract(column))
  } catch {
    case e: Exception => {
      logger.warn("Failed during processing", e)
    }
  }

  column.features.asScala.foreach(f => {
    System.out.println("%s:%f".format(f.name, f.value));
  })
}
