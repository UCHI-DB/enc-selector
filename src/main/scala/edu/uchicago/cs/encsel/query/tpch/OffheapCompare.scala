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
import java.nio.ByteBuffer

import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper
import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper.ReaderProcessor
import org.apache.parquet.VersionParser
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ValuesType.{DEFINITION_LEVEL, REPETITION_LEVEL}
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page._
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.io.ParquetDecodingException

import scala.collection.JavaConversions._

class OffheapPageVisitor(path: ColumnDescriptor, pageValueCount: Int) extends Visitor[ByteBuffer] {
  override def visit(page: DataPageV1): ByteBuffer = {
    val rlReader = page.getRlEncoding.getValuesReader(path, REPETITION_LEVEL)
    val dlReader = page.getDlEncoding.getValuesReader(path, DEFINITION_LEVEL)
    try {
      val bytes = page.getBytes.toByteBuffer
      rlReader.initFromPage(pageValueCount, bytes, 0)
      var next = rlReader.getNextOffset
      dlReader.initFromPage(pageValueCount, bytes, next)
      next = dlReader.getNextOffset
      // TODO Read data from bytes starting at next

      null
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }

  override def visit(page: DataPageV2): ByteBuffer = {
    try {
      // TODO Read data from bytes starting at 0

      null
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }
}

class Offheap {
  ParquetReaderHelper.read(new File("/home/harper/TPCH/lineitem.parquet").toURI, new ReaderProcessor() {
    override def processFooter(footer: Footer): Unit = {

    }

    override def processRowGroup(version: VersionParser.ParsedVersion,
                                 meta: BlockMetaData,
                                 rowGroup: PageReadStore): Unit = {
      val cd = TPCHSchema.lineitemSchema.getColumns()(1)
      val pageReader = rowGroup.getPageReader(cd)
      var page: DataPage = null
      while ((page = pageReader.readPage()) != null) {
        page.accept(new OffheapPageVisitor(cd, page.getValueCount))
      }

    }
  })
}

class Onheap {

}