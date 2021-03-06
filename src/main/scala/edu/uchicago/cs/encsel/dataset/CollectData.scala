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

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import edu.uchicago.cs.encsel.Config
import edu.uchicago.cs.encsel.dataset.column.{Column, ColumnReader, ColumnReaderFactory}
import edu.uchicago.cs.encsel.dataset.feature.Features
import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.schema.Schema
import edu.uchicago.cs.encsel.util.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
  * Created by harper on 4/23/17.
  */
object CollectData extends App {
  val f = new File(args(0)).toURI
  val threadPool = Executors.newFixedThreadPool(Config.collectorThreadCount)
  val dataCollector = new DataCollector
  FileUtils.multithread_scan(f, path => dataCollector.collect(path.toUri), threadPool)
}


class DataCollector {
  var persistence: Persistence = Persistence.get
  val logger = LoggerFactory.getLogger(this.getClass)

  def collect(source: URI): Unit = {
    val path = Paths.get(source)
    try {
      if (Files.isDirectory(path)) {
        logger.warn("Running on Directory is undefined")
        return
      }
      if (logger.isDebugEnabled())
        logger.debug("Scanning " + source.toString)

      if (isDone(source)) {
        if (logger.isDebugEnabled())
          logger.debug("Scanned mark found, skip")
        return
      }

      val columner: ColumnReader = ColumnReaderFactory.getColumnReader(source)
      if (columner == null) {
        if (logger.isDebugEnabled())
          logger.debug("No available reader found, skip")
        return
      }
      val defaultSchema = Schema.getSchema(source)
      if (null == defaultSchema) {
        if (logger.isDebugEnabled())
          logger.debug("Schema not found, skip")
        return
      }
      val columns = columner.readColumn(source, defaultSchema)

//      columns.foreach(extractFeature)

      persistence.save(columns)

      markDone(source)
      if (logger.isDebugEnabled())
        logger.debug("Scanned " + source.toString)

    } catch {
      case e: Exception => logger.error("Exception while scanning " + source.toString, e)
    }
  }

  protected def isDone(file: URI): Boolean = {
    FileUtils.isDone(file, "done")
  }

  protected def markDone(file: URI) = {
    FileUtils.markDone(file, "done")
  }

  private def extractFeature(col: Column): Unit = {
    try {
      col.features.addAll(Features.extract(col).toSet.asJavaCollection)
    } catch {
      case e: Exception => logger.warn("Exception while processing column:%s@%s".format(col.colName, col.origin), e)
    }
  }

}