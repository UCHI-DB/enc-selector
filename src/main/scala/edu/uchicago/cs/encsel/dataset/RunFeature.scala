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

package edu.uchicago.cs.encsel.dataset

import java.util.concurrent.atomic.AtomicInteger

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature._
import edu.uchicago.cs.encsel.dataset.feature.encode.orc.OrcEncFileSize
import edu.uchicago.cs.encsel.dataset.feature.encode.parquet.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import edu.uchicago.cs.encsel.model.DataType
import org.slf4j.LoggerFactory

/**
  * Created by harper on 4/23/17.
  */
class FeatureRunner {

  var missed = Set[FeatureExtractor]()

  def run(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val persist = new JPAPersistence

    val prefix = args.length match {
      case gt if gt > 0 => args(0)
      case _ => ""
    }

    val filter = args.length match {
      case gt if gt > 0 =>
        args(1) match {
          case "none" => null
          case "firstn" => Filter.firstNFilter(args(2).toInt)
          case "iid" => Filter.iidSamplingFilter(args(2).toDouble)
          case "size" => Filter.sizeFilter(args(2).toInt)
          case "minsize" => Filter.minSizeFilter(args(2).toInt, args(3).toDouble)
          case _ => throw new IllegalArgumentException(args(1))
        }
      case _ => null
    }

    Features.extractors.clear()
    Features.extractors ++= missed

    val persistence = Persistence.get
    val columns = getColumns(persistence)

    val size = columns.size
    var counter = new AtomicInteger(0)

    columns.toList.par.foreach(column => {
      val current = counter.addAndGet(1)
      System.out.println("Processing %d / %d : %s".format(current, size, column.colFile))
      try {
        if (filter == null) {
          column.replaceFeatures(Features.extract(column))
        } else {
          column.replaceFeatures(Features.extract(column, filter, prefix))
        }
        persistence.save(Seq(column))
      } catch {
        case e: Exception => {
          logger.warn("Failed during processing", e)
        }
      }
    })
  }

  def getColumns(persistence: Persistence): List[Column] = {
    val columns = persistence.load().filter(col => col.dataType == DataType.INTEGER || col.dataType == DataType.STRING).toList
    columns
  }
}

object RunFeature extends App {
  val featureRunner = new FeatureRunner()
  featureRunner.missed = Set(ParquetEncFileSize)
  featureRunner.run(args)
}
