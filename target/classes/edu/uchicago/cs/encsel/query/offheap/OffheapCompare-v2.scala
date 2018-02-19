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

package edu.uchicago.cs.encsel.query.offheap

import java.io.File

import edu.uchicago.cs.encsel.query.offheap.EqualJniScalar
import edu.uchicago.cs.encsel.parquet.{EncReaderProcessor, ParquetReaderHelper}
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap
import edu.uchicago.cs.encsel.query.tpch._
import org.apache.parquet.VersionParser
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConversions._

object MyOffheap extends App {

  val entryWidth = 17
  val cd = TPCHSchema.orderSchema.getColumns()(1)

  //test("JniScalar", new EqualJniScalar(5000, entryWidth))
  test("ScalarDM", new EqualScalar(65486, entryWidth, true))
  test("ScalarFM", new EqualScalar(65486, entryWidth, false))
  test("Int", new EqualInt(65486, entryWidth))
  test("Long", new EqualLong(65486, entryWidth))

  def test(name: String, pred: Predicate): Unit = {
    val predVisitor = new PredicateVisitor(cd, pred)
    val repeat = 20
    var clocktime = 0L
    var cputime = 0L
    var usertime = 0L
    for (i <- 0 until repeat) {
      val prof = ParquetReaderHelper.profile(new File("/Users/chunwei/research/tpch/orders.parquet").toURI, new EncReaderProcessor() {
        override def processRowGroup(version: VersionParser.ParsedVersion,
                                     meta: BlockMetaData,
                                     rowGroup: PageReadStore): Unit = {
          val pageReader = rowGroup.getPageReader(cd)
          var page = pageReader.readPage()
          while (page != null) {
            val res = page.accept(predVisitor)
            page = pageReader.readPage()
            res.clear()
          }
        }
      })
      clocktime = clocktime + prof.wallclock
      cputime = cputime + prof.cpu
      usertime = usertime + prof.user
    }

    println("%s,%d,%d,%d".format(name, clocktime / repeat, cputime / repeat, usertime / repeat))
  }
}

object MyOnheap extends App {

  val pred: Any => Boolean = (data: Any) => {
    data.toString.toInt == 1
  }
  val cd = TPCHSchema.orderSchema.getColumns()(1)

  val repeat = 20
  var clocktime = 0L
  var cputime = 0L
  var usertime = 0L
  for (i <- 0 until repeat) {
    val prof = ParquetReaderHelper.profile(new File("/Users/chunwei/research/tpch/orders.parquet").toURI, new EncReaderProcessor() {

      override def processRowGroup(version: VersionParser.ParsedVersion,
                                   meta: BlockMetaData,
                                   rowGroup: PageReadStore): Unit = {
        //ParquetFileReader.setColFilter(pageReaders, descriptor, pred);

        val colReader = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), new NonePrimitiveConverter, version);
        val bitmap = new RoaringBitmap()
        for (i <- 0L until rowGroup.getRowCount) {
          bitmap.set(i, pred(colReader.getInteger))
          colReader.consume
        }
      }
    })

    clocktime = clocktime + prof.wallclock
    cputime = cputime + prof.cpu
    usertime = usertime + prof.user
  }
  println("%s,%d,%d,%d".format("Onheap", clocktime / repeat, cputime / repeat, usertime / repeat))
}