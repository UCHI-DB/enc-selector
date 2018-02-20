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
package edu.uchicago.cs.encsel.dataset.feature.report

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import edu.uchicago.cs.encsel.classify.nn.NNPredictor
import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor, Features}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.perf.Profiler
import org.apache.commons.io.IOUtils

/**
  * Time usage for selecting encoding (including feature extraction)
  */
object SelTimeUsage extends FeatureExtractor {

  val profiler = new Profiler
  val intPredictor = new NNPredictor("src/main/nnmodel/int_model", 19)
  val stringPredictor = new NNPredictor("src/main/nnmodel/string_model", 19)

  override def featureType: String = "SelTimeUsage"

  override def supportFilter: Boolean = true

  override def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)


    column.dataType match {
      case DataType.INTEGER => {
        val buffer = new ByteArrayOutputStream
        IOUtils.copy(input, buffer)
        val data = buffer.toByteArray

        profiler.reset
        profiler.mark
        val features = Features.extractors.flatMap(ex => {
          ex.extract(column, new ByteArrayInputStream(data), prefix)
        }).map(_.value).toArray
        profiler.pause
        val time1 = profiler.stop

        profiler.reset
        profiler.mark
        intPredictor.predict(features)

        profiler.pause
        val time2 = profiler.stop
        Iterable(
          new Feature(fType, "cpu1", time1.cpu),
          new Feature(fType, "wc1", time1.wallclock),
          new Feature(fType, "user1", time1.user),
          new Feature(fType, "cpu2", time2.cpu),
          new Feature(fType, "wc2", time2.wallclock),
          new Feature(fType, "user2", time2.user)
        )
      }
      case DataType.STRING => {
        val buffer = new ByteArrayOutputStream
        IOUtils.copy(input, buffer)
        val data = buffer.toByteArray

        profiler.reset
        profiler.mark

        // Instead of reding file from disk for each features, load file in memory for reuse
        val features = Features.extractors.flatMap(ex => {
          ex.extract(column, new ByteArrayInputStream(data), prefix)
        }).map(_.value).toArray
        profiler.pause
        val time1 = profiler.stop

        profiler.reset
        profiler.mark
        stringPredictor.predict(features)

        profiler.pause
        val time2 = profiler.stop

        Iterable(
          new Feature(fType, "cpu1", time1.cpu),
          new Feature(fType, "wc1", time1.wallclock),
          new Feature(fType, "user1", time1.user),
          new Feature(fType, "cpu2", time2.cpu),
          new Feature(fType, "wc2", time2.wallclock),
          new Feature(fType, "user2", time2.user)
        )
      }
      case _ => Iterable()
    }
  }
}