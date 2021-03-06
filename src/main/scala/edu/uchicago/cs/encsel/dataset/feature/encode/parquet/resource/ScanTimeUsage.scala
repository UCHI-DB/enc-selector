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

package edu.uchicago.cs.encsel.dataset.feature.encode.parquet.resource

import java.io.{File, InputStream}
import java.net.URI

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.model.DataType._
import edu.uchicago.cs.encsel.model.{FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import edu.uchicago.cs.encsel.query.VColumnPredicate
import edu.uchicago.cs.encsel.query.operator.VerticalSelect
import edu.uchicago.cs.encsel.query.tpch.NostoreColumnTempTable
import edu.uchicago.cs.encsel.util.perf.Profiler
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.slf4j.LoggerFactory

object ScanTimeUsage extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "ScanTimeUsage"

  def supportFilter: Boolean = false

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {

    val select = new VerticalSelect() {
      override def createRecorder(schema: MessageType) = new NostoreColumnTempTable(schema)
    };
    val predicate = new VColumnPredicate((data) => true, 0)
    val profiler = new Profiler

    val encFunction: (String, MessageType) => Iterable[Feature] =
      (encoding: String, schema: MessageType) => {
        try {
          val fileName = col.colFile + "." + encoding;
          val encfile = new URI(fileName)

          if (!new File(encfile).exists())
            return Iterable[Feature]()

          profiler.reset
          profiler.mark
          select.select(encfile, predicate, schema, Array(0))
          profiler.pause
          val time = profiler.stop

          Iterable(
            new Feature(featureType, "%s_wallclock".format(encoding), time.wallclock),
            new Feature(featureType, "%s_cpu".format(encoding), time.cpu),
            new Feature(featureType, "%s_user".format(encoding), time.user)
          )
        } catch {
          case e: Exception => {
            e.printStackTrace()
            Iterable[Feature]()
          }
        }
      }

    col.dataType match {
      case INTEGER => {
        val schema = new MessageType("default",
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value")
        )
        IntEncoding.values().filter(_.parquetEncoding() != null)
          .flatMap(encoding => encFunction(encoding.name(), schema))
      }
      case STRING => {
        val schema = new MessageType("default",
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value")
        )
        StringEncoding.values().filter(_.parquetEncoding() != null)
          .flatMap(encoding => encFunction(encoding.name(), schema))
      }
      case DOUBLE => {
        val schema = new MessageType("default",
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value")
        )
        FloatEncoding.values().filter(_.parquetEncoding() != null)
          .flatMap(encoding => encFunction(encoding.name(), schema))
      }
      case LONG => {
        val schema = new MessageType("default",
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value")
        )
        LongEncoding.values().filter(_.parquetEncoding() != null)
          .flatMap(encoding => encFunction(encoding.name(), schema))
      }
      case BOOLEAN => {
        Iterable[Feature]()
      }
      case FLOAT => {
        val schema = new MessageType("default",
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value")
        )
        FloatEncoding.values().filter(_.parquetEncoding() != null)
          .flatMap(encoding => encFunction(encoding.name(), schema))
      }
    }
  }

}
