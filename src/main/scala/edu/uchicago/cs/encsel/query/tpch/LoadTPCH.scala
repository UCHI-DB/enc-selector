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

import edu.uchicago.cs.encsel.parquet.{EncContext, EncReaderProcessor, ParquetReaderHelper, ParquetWriterHelper}
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter
import edu.uchicago.cs.encsel.query.tpch.LoadTPCH.{folder, inputsuffix, outputsuffix}
import org.apache.parquet.VersionParser
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConversions._

object LoadTPCH extends App {

  //val folder = "/home/harper/TPCH/"
  val folder = "../tpch-generator/dbgen/"
  //  val folder = args(0)
  val inputsuffix = ".tbl"
  val outputsuffix = ".parquet"

  // Load TPCH
  TPCHSchema.schemas.foreach(schema => {
    ParquetWriterHelper.write(
      new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
      schema,
      new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI, "\\|", false)
  })
}

object LoadPart20 extends App {
  val schema = TPCHSchema.partSchema
  var counter = 0
  schema.getColumns.forEach(cd => {
    EncContext.encoding.get().put(cd.toString, Encoding.PLAIN_DICTIONARY)
    EncContext.context.get().put(cd.toString, Array[AnyRef](counter.toString, (counter * 10).toString))
    counter += 1
  })

  ParquetWriterHelper.write(
    new File("src/test/resource/parquet/part_20").toURI,
    schema,
    new File("src/test/resource/parquet/part_20.parquet").toURI, "\\|", false)
}

object LoadTPCH4Offheap extends App {
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.BIT_PACKED)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("6", "50"))
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}

object  tes extends App {
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(8).toString, Encoding.PLAIN_DICTIONARY)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(8).toString, Array[AnyRef]("6", "50"))
  var dictMap = ParquetWriterHelper.buildGlobalDict(new File("../tpch-generator/dbgen/lineitem.tbl").toURI,8,TPCHSchema.lineitemSchema)
  println(dictMap)
  EncContext.globalDict.get().put(TPCHSchema.lineitemSchema.getColumns()(8).toString, dictMap)
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}


object LoadTPCH4Plain extends App {
  var compression = "UNCOMPRESSED"
  if (args.length == 0) {
    println("dude, i need at least one parameter for compression type (UNCOMPRESSED by default)")
  }
  else {
    compression = args(0)
  }
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Encoding.PLAIN)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("50", "50"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Array[AnyRef]("string", "string"))
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false, compression)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}

object LoadTPCH4Best extends App {
  var compression = "UNCOMPRESSED"
  if (args.length == 0) {
    println("dude, i need at least one parameter for compression type (UNCOMPRESSED by default)")
  }
  else {
    compression = args(0)
  }
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.PLAIN_DICTIONARY)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Encoding.PLAIN_DICTIONARY)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Encoding.PLAIN_DICTIONARY)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("50", "50"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Array[AnyRef]("string", "string"))
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false, compression)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}

object LoadTPCH4Worst extends App {
  var compression = "UNCOMPRESSED"
  if (args.length == 0) {
    println("dude, i need at least one parameter for compression type (UNCOMPRESSED by default)")
  }
  else {
    compression = args(0)
  }
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Encoding.PLAIN_DICTIONARY)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Encoding.PLAIN)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("50", "50"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Array[AnyRef]("0.4", "0.4"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Array[AnyRef]("string", "string"))
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false, compression)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}


object LoadTPCH4AllPlain extends App {
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(0).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(1).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(2).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(3).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(7).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(8).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(9).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(11).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(12).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(13).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(14).toString, Encoding.PLAIN)
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(15).toString, Encoding.PLAIN)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(0).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(1).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(2).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(3).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(5).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(6).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(7).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(8).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(9).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(10).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(11).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(12).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(13).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(14).toString, Array[AnyRef]("21", "2000000"))
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(15).toString, Array[AnyRef]("21", "2000000"))
  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}

object LoadTPCH4Default extends App {

  ParquetWriterHelper.write(new File("../tpch-generator/dbgen/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("../tpch-generator/dbgen/lineitem.parquet").toURI, "\\|", false)
  ParquetReaderHelper.getColSize(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, 4)
}

object LoadLineItem extends App {
  val schema = TPCHSchema.orderSchema

  EncContext.encoding.get().put(schema.getColumns()(1).toString, Encoding.PLAIN)

  ParquetWriterHelper.write(
    new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
    schema,
    new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI, "\\|", false)
}


object SelectLineitem extends App {
  val schema = TPCHSchema.lineitemOptSchema
  val start = System.currentTimeMillis()
  ParquetReaderHelper.read(new File("../tpch-generator/dbgen/lineitem.parquet").toURI, new EncReaderProcessor() {

    override def processRowGroup(version: VersionParser.ParsedVersion,
                                 meta: BlockMetaData, rowGroup: PageReadStore): Unit = {

      val readers = schema.getColumns.map(col => {
        new ColumnReaderImpl(col, rowGroup.getPageReader(col), new NonePrimitiveConverter, version)
      })

      readers.foreach(reader => {
        for (i <- 0L until rowGroup.getRowCount) {
          if (reader.getCurrentDefinitionLevel > 0)
            reader.readValue()
          reader.consume()
        }
      })
    }
  })
  println(System.currentTimeMillis() - start)
}