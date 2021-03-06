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
package edu.uchicago.cs.encsel.dataset.parser.csv

import java.io.{InputStream, InputStreamReader}

import edu.uchicago.cs.encsel.dataset.parser.{Parser, Record}
import edu.uchicago.cs.encsel.dataset.schema.Schema
import org.apache.commons.csv.{CSVFormat, CSVRecord}

import scala.collection.JavaConversions.asScalaIterator

class CommonsCSVParser extends Parser {

  var format: CSVFormat = CSVFormat.EXCEL

  override def parse(input: InputStream, schema: Schema): Iterator[Record] = {
    this.schema = schema
    val reader = new InputStreamReader(input)
    val parser = format.parse(reader)

    val csvrecords = parser.iterator()
    csvrecords.map(new CSVRecordWrapper(_))
  }
}

class CSVRecordWrapper(inner: CSVRecord) extends Record {
  var innerRecord = inner

  def apply(idx: Int): String = {
    inner.get(idx)
  }

  def length(): Int = {
    inner.size()
  }

  override def toString: String = {
    inner.toString
  }

  def iterator(): Iterator[String] = {
    inner.iterator()
  }
}