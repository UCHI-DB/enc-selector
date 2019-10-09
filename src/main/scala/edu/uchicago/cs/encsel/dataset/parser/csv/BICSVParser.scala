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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */
package edu.uchicago.cs.encsel.dataset.parser.csv

import java.io.{BufferedReader, File, FileReader, InputStream, InputStreamReader}
import java.net.URI

import edu.uchicago.cs.encsel.dataset.parser.{Parser, Record}
import edu.uchicago.cs.encsel.dataset.schema.Schema
import org.apache.commons.csv.{CSVFormat, CSVRecord}

import scala.collection.JavaConversions.asScalaIterator

class BICSVParser extends CommonsCSVParser {

  override def parse(inputFile: URI, schema: Schema): Iterator[Record] = {
    val bufferedReader = new BufferedReader(new FileReader(new File(inputFile)))
    val firstLine = bufferedReader.readLine()
    val split = firstLine.split('|')
    guessedHeader = (0 until split.length).map("f%d".format(_)).toArray

    var format = CSVFormat.DEFAULT.withDelimiter('|').withQuote(0x13.toChar)
      .withIgnoreEmptyLines(true).withHeader(guessedHeader: _*)

    if (firstLine.contains("null|null|null|null")) {
      format = format.withSkipHeaderRecord(true)
    }

    this.format = format

    super.parse(inputFile, schema)
  }

  override def hasHeaderInFile = false
}

