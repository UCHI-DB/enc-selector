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

import java.io.{File, IOException}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import javax.management.MBeanServerFactory

import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper
import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper.ReaderProcessor
import edu.uchicago.cs.encsel.query.tpch.Offheap.{cputime, mbean, usertime}
import edu.uchicago.cs.encsel.query.{Bitmap, NonePrimitiveConverter}
import org.apache.parquet.VersionParser
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ValuesType.{DEFINITION_LEVEL, REPETITION_LEVEL}
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.io.ParquetDecodingException

import scala.collection.JavaConversions._

trait Predicate {
  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer
}

class EqualScalar(val target: Int, val entryWidth: Int) extends Predicate {
  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val result = ByteBuffer.allocateDirect(Math.ceil(size.toDouble / 8).toInt)

    var counter = 0
    var buffer: Byte = 0
    for (i <- 0 until size) {
      val index = size * entryWidth / 32
      val offset = size * entryWidth % 32
      val byteIndex = size % 8

      val intValue = input.getInt(offset + index * 4) >> offset
      val mask = ((1L << entryWidth) - 1)

      intValue & mask ^ target match {
        case 0 => buffer = (buffer | (1 << counter)).toByte
        case _ => {}
      }

      counter += 1
      if (counter == 8) {
        counter = 0
        result.put(buffer)
        buffer = 0
      }
    }
    return result
  }
}

class EqualInt(val target: Int, val entryWidth: Int) extends Predicate {

  val values = new Array[Int](32)

  {
    var origin = 0
    for (i <- 0 until 32 / entryWidth) {
      origin = origin | (target << i * entryWidth)
    }

    for (i <- 0 until 32) {
      val mask = (1 << i) - 1
      values(i) = (origin << i) | ((origin >> (32 - i)) & mask)
    }
  }


  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val byteLength = Math.ceil((entryWidth * size.toDouble) / 8).toInt
    val dest = ByteBuffer.allocateDirect(byteLength)

    val intLength = byteLength / 4

    input.position(offset)
    for (i <- 0 until intLength) {
      val bitoff = entryWidth - (i * 32 % entryWidth)
      dest.putInt(i, input.getInt ^ values(bitoff))
    }
    // get remaining bytes
    var remain = 0
    val remainBytes = byteLength % 4
    val remainStart = entryWidth - (intLength * 32 % entryWidth)
    for (i <- 0 until remainBytes) {
      remain = remain | (input.get << (i * 8))
    }
    val res = remain ^ values(remainStart)
    for (i <- 0 until remainBytes) {
      dest.put((remain >> i * 8).asInstanceOf[Byte])
    }

    return dest
  }
}

class EqualLong(val target: Int, val entryWidth: Int) extends Predicate {

  val values = new Array[Long](64)

  {
    var origin = 0L
    for (i <- 0 until 64 / entryWidth) {
      origin = origin | (target << i * entryWidth)
    }

    for (i <- 0 until 64) {
      val mask = (1 << i) - 1
      values(i) = (origin << i) | ((origin >> (64 - i)) & mask)
    }
  }


  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val byteLength = Math.ceil((entryWidth * size.toDouble) / 8).toInt
    val dest = ByteBuffer.allocateDirect(byteLength)

    val longLength = byteLength / 8

    input.position(offset)
    for (i <- 0 until longLength) {
      val bitoff = entryWidth - (i * 64 % entryWidth)
      dest.putLong(i, input.getLong ^ values(bitoff))
    }
    // get remaining bytes
    var remain = 0
    val remainBytes = byteLength % 4
    val remainStart = entryWidth - (longLength * 64 % entryWidth)
    for (i <- 0 until remainBytes) {
      remain = remain | (input.get << (i * 8))
    }
    val res = remain ^ values(remainStart)
    for (i <- 0 until remainBytes) {
      dest.put((remain >> i * 8).asInstanceOf[Byte])
    }

    return dest
  }
}

class PredicateVisitor(path: ColumnDescriptor, pred: Predicate) extends Visitor[ByteBuffer] {
  override def visit(page: DataPageV1): ByteBuffer = {
    val rlReader = page.getRlEncoding.getValuesReader(path, REPETITION_LEVEL)
    val dlReader = page.getDlEncoding.getValuesReader(path, DEFINITION_LEVEL)
    val pageValueCount = page.getValueCount
    try {
      val bytes = page.getBytes.toByteBuffer
      rlReader.initFromPage(pageValueCount, bytes, 0)
      var next = rlReader.getNextOffset
      dlReader.initFromPage(pageValueCount, bytes, next)
      next = dlReader.getNextOffset
      // Read data from bytes starting at next
      // Generate a byte buffer containing the equality
      return pred.execute(bytes, next, pageValueCount);
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }

  override def visit(page: DataPageV2): ByteBuffer = {
    try {
      // Read data from bytes starting at 0
      val bytes = page.getData.toByteBuffer
      return pred.execute(bytes, 0, page.getValueCount)
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }
}

object Offheap extends App {
  val entryWidth = 18
  val pred = new EqualLong(5000, entryWidth)
  val cd = TPCHSchema.lineitemSchema.getColumns()(1)
  val predVisitor = new PredicateVisitor(cd, pred)


  val mbean = ManagementFactory.getThreadMXBean
  val repeat = 10
  var clocktime = 0L
  var cputime = 0L
  var usertime = 0L
  for (i <- 0 until repeat) {
    val clockstart = System.currentTimeMillis
    val cpustart = mbean.getCurrentThreadCpuTime
    val userstart = mbean.getCurrentThreadUserTime

    ParquetReaderHelper.read(new File("/home/harper/TPCH/lineitem.parquet").toURI, new ReaderProcessor() {
      override def processFooter(footer: Footer): Unit = {

      }

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
    clocktime = clocktime + (System.currentTimeMillis() - clockstart)
    cputime = cputime + (mbean.getCurrentThreadCpuTime - cpustart)
    usertime = usertime + (mbean.getCurrentThreadUserTime - userstart)
  }

  println(clocktime / repeat)
  println(cputime / repeat)
  println(usertime / repeat)
}

object Onheap extends App {

  val pred: Any => Boolean = (data: Any) => {
    data.toString.toInt == 5000
  }
  val cd = TPCHSchema.lineitemSchema.getColumns()(1)

  val mbean = ManagementFactory.getThreadMXBean
  val repeat = 10
  var clocktime = 0L
  var cputime = 0L
  var usertime = 0L
  for (i <- 0 until repeat) {
    val clockstart = System.currentTimeMillis
    val cpustart = mbean.getCurrentThreadCpuTime
    val userstart = mbean.getCurrentThreadUserTime

    ParquetReaderHelper.read(new File("/home/harper/TPCH/lineitem.parquet").toURI, new ReaderProcessor() {
      override def processFooter(footer: Footer): Unit = {

      }

      override def processRowGroup(version: VersionParser.ParsedVersion,
                                   meta: BlockMetaData,
                                   rowGroup: PageReadStore): Unit = {
        val colReader = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), new NonePrimitiveConverter, version);
        val bitmap = new Bitmap(rowGroup.getRowCount)
        for (i <- 0L until rowGroup.getRowCount) {
          bitmap.set(i, pred(colReader.getInteger))
          colReader.consume
        }
      }
    })

    clocktime = clocktime + (System.currentTimeMillis() - clockstart)
    cputime = cputime + (mbean.getCurrentThreadCpuTime - cpustart)
    usertime = usertime + (mbean.getCurrentThreadUserTime - userstart)
  }
  println(clocktime / repeat)
  println(cputime / repeat)
  println(usertime / repeat)
}