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

package edu.uchicago.cs.encsel.dataset.feature

import java.io.File
import java.lang.management.ManagementFactory

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType._
import edu.uchicago.cs.encsel.model.{FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import edu.uchicago.cs.encsel.query.{HColumnPredicate, VColumnPredicate}
import edu.uchicago.cs.encsel.query.operator.{HorizontalSelect, VerticalSelect}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.slf4j.LoggerFactory

object ScanTimeUsage extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "ScanTimeUsage"

  def supportFilter: Boolean = false

  def extract(col: Column, prefix: String): Iterable[Feature] = {

    val select = new VerticalSelect();
    val predicate = new VColumnPredicate((data) => true, 0)
    val timembean = ManagementFactory.getThreadMXBean;

    col.dataType match {
      case INTEGER => {
        var schema = new MessageType("default",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "data")
        )
        IntEncoding.values().flatMap(encoding => {
          try {
            val fileName = col.colFile + "." + encoding.name();
            val startcpu = timembean.getCurrentThreadCpuTime
            val startuser = timembean.getCurrentThreadUserTime

            select.select(new File(fileName).toURI, predicate, schema, Array(0))

            val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
            val userconsumption = timembean.getCurrentThreadUserTime - startuser

            Iterable(
              new Feature(featureType, "%s_cpu".format(encoding.name()), cpuconsumption),
              new Feature(featureType, "%s_user".format(encoding.name()), userconsumption)
            )
          } catch {
            case e: Exception => {
              e.printStackTrace()
              Iterable[Feature]()
            }
          }
        })
      }
      case STRING => {
        var schema = new MessageType("default",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "data")
        )
        StringEncoding.values().flatMap(encoding => {
          try {
            val fileName = col.colFile + "." + encoding.name();
            val startcpu = timembean.getCurrentThreadCpuTime
            val startuser = timembean.getCurrentThreadUserTime

            select.select(new File(fileName).toURI, predicate, schema, Array(0))

            val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
            val userconsumption = timembean.getCurrentThreadUserTime - startuser

            Iterable(
              new Feature(featureType, "%s_cpu".format(encoding.name()), cpuconsumption),
              new Feature(featureType, "%s_user".format(encoding.name()), userconsumption)
            )
          } catch {
            case e: Exception => {
              Iterable[Feature]()
            }
          }
        })
      }
      case DOUBLE => {
        var schema = new MessageType("default",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "data")
        )
        FloatEncoding.values().flatMap(encoding => {
          try {
            val fileName = col.colFile + "." + encoding.name();
            val startcpu = timembean.getCurrentThreadCpuTime
            val startuser = timembean.getCurrentThreadUserTime

            select.select(new File(fileName).toURI, predicate, schema, Array(0))

            val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
            val userconsumption = timembean.getCurrentThreadUserTime - startuser

            Iterable(
              new Feature(featureType, "%s_cpu".format(encoding.name()), cpuconsumption),
              new Feature(featureType, "%s_user".format(encoding.name()), userconsumption)
            )
          } catch {
            case e: Exception => {
              Iterable[Feature]()
            }
          }
        })
      }
      case LONG => {
        var schema = new MessageType("default",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "data")
        )
        LongEncoding.values().flatMap(encoding => {
          try {
            val fileName = col.colFile + "." + encoding.name();
            val startcpu = timembean.getCurrentThreadCpuTime
            val startuser = timembean.getCurrentThreadUserTime

            select.select(new File(fileName).toURI, predicate, schema, Array(0))

            val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
            val userconsumption = timembean.getCurrentThreadUserTime - startuser

            Iterable(
              new Feature(featureType, "%s_cpu".format(encoding.name()), cpuconsumption),
              new Feature(featureType, "%s_user".format(encoding.name()), userconsumption)
            )
          } catch {
            case e: Exception => {
              Iterable[Feature]()
            }
          }
        })
      }
      case BOOLEAN => {
        Iterable[Feature]()
      }
      case FLOAT => {
        var schema = new MessageType("default",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.FLOAT, "data")
        )
        FloatEncoding.values().flatMap(encoding => {
          try {
            val fileName = col.colFile + "." + encoding.name();
            val startcpu = timembean.getCurrentThreadCpuTime
            val startuser = timembean.getCurrentThreadUserTime

            select.select(new File(fileName).toURI, predicate, schema, Array(0))

            val cpuconsumption = timembean.getCurrentThreadCpuTime - startcpu
            val userconsumption = timembean.getCurrentThreadUserTime - startuser

            Iterable(
              new Feature(featureType, "%s_cpu".format(encoding.name()), cpuconsumption),
              new Feature(featureType, "%s_user".format(encoding.name()), userconsumption)
            )
          } catch {
            case e: Exception => {
              Iterable[Feature]()
            }
          }
        })
      }
    }

  }

}
