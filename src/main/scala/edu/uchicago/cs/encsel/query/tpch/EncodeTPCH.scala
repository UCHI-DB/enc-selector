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

package edu.uchicago.cs.encsel.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.parquet.{EncContext, ParquetWriterHelper}
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.metadata.CompressionCodecName


object EncodeTPCH extends App {


  def plainGzip(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    TPCHSchema.schemas.foreach(schema => {
      schema.getColumns.forEach(cd=>{
        EncContext.encoding.get().put(cd.toString, Encoding.PLAIN)
        EncContext.context.get().put(cd.toString, Array[Object]("0","0"))
      })
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false,
        CompressionCodecName.GZIP)
    })
  }

  def dictGzip(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    TPCHSchema.schemas.foreach(schema => {
      schema.getColumns.forEach(cd=>{
        EncContext.encoding.get().put(cd.toString, Encoding.PLAIN_DICTIONARY)
      })
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false,
        CompressionCodecName.GZIP)
    })
  }

  def parquetGzip(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    TPCHSchema.schemas.foreach(schema => {
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false,
        CompressionCodecName.GZIP)
    })
  }

  def plain(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    // Load TPCH
    TPCHSchema.schemas.foreach(schema => {
      schema.getColumns.forEach(cd=>{
        EncContext.encoding.get().put(cd.toString, Encoding.PLAIN)
        EncContext.context.get().put(cd.toString, Array[Object]("0","0"))
      })
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false)
    })
  }

  def dict(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    TPCHSchema.schemas.foreach(schema => {
      schema.getColumns.forEach(cd=>{
        EncContext.encoding.get().put(cd.toString, Encoding.PLAIN_DICTIONARY)
      })
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false)
    })
  }

  def parquet(folder: String, inputsuffix: String, outputsuffix: String): Unit = {
    TPCHSchema.schemas.foreach(schema => {
      schema.getColumns.forEach(cd=>{
        EncContext.encoding.get().remove(cd.toString)
      })
      ParquetWriterHelper.write(
        new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
        schema,
        new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
        "\\|",
        false)
    })
  }

  val folder = args(0)
  val inputsuffix = ".tbl"

  parquet(folder, ".tbl", ".pq.parquet")
  parquetGzip(folder, ".tbl", ".pqg.parquet")

}
