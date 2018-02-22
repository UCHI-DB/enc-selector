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

import java.io.{File, InputStream}
import java.lang.management.ManagementFactory
import java.net.URI

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.model.DataType._
import edu.uchicago.cs.encsel.model.{FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import edu.uchicago.cs.encsel.query.VColumnPredicate
import edu.uchicago.cs.encsel.query.operator.VerticalSelect
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.slf4j.LoggerFactory

object ScanTimeUsage extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "ScanTimeUsage"

  def supportFilter: Boolean = false

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {

    val select = new VerticalSelect();
    val predicate = new VColumnPredicate((data) => true, 0)
    val timembean = ManagementFactory.getThreadMXBean;

    val encFunction: (String, MessageType) => Iterable[Feature] =
      (encoding: String, schema: MessageType) => {
        try {
          val fileName = col.colFile + "." + encoding;
          val encfile = new URI(fileName)

          if (!new File(encfile).exists())
            return Iterable[Feature]()

          val startcpu = timembean.getCurrentThreadCpuTime
          val startuser = timembean.getCurrentThreadUserTime
          val startwc = System.currentTimeMillis()
          select.select(encfile, predicate, schema, Array(0))

          val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
          val userconsumption = timembean.getCurrentThreadUserTime - startuser
          val wcconsumption = System.currentTimeMillis() - startwc

          Iterable(
            new Feature(featureType, "%s_wallclock".format(encoding), wcconsumption),
            new Feature(featureType, "%s_cpu".format(encoding), cpuconsumption),
            new Feature(featureType, "%s_user".format(encoding), userconsumption)
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
