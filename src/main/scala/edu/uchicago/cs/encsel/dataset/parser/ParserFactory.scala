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
package edu.uchicago.cs.encsel.dataset.parser

import java.io.{BufferedReader, File, FileInputStream, FileReader}
import java.net.URI

import edu.uchicago.cs.encsel.dataset.parser.col.ColParser
import edu.uchicago.cs.encsel.dataset.parser.csv.CommonsCSVParser
import edu.uchicago.cs.encsel.dataset.parser.excel.XLSXParser
import edu.uchicago.cs.encsel.dataset.parser.json.LineJsonParser
import edu.uchicago.cs.encsel.dataset.parser.tsv.TSVParser
import edu.uchicago.cs.encsel.dataset.schema.SchemaGuesser
import edu.uchicago.cs.encsel.model.DataType
import org.apache.commons.csv.{CSVFormat, QuoteMode}

object ParserFactory {

  def getParser(source: URI): Parser = {
    source.getScheme match {
      case "file" => {
        source.getPath match {
          case x if x.toLowerCase().endsWith("csv") => {
            guessCSVParser(source, Array(',', '|'))
          }
          case x if x.toLowerCase().endsWith("tsv") => {
            guessCSVParser(source, Array('\t'))
          }
          case x if x.toLowerCase().endsWith("json") => {
            new LineJsonParser
          }
          case x if x.toLowerCase().endsWith("xlsx") => {
            new XLSXParser
          }
          case x if x.toLowerCase().endsWith("tmp") => {
            new ColParser
          }
          case _ =>
            null
        }
      }
      case _ =>
        null
    }
  }

  def guessCSVParser(source: URI, separators: Array[Char]) = {
    val reader = new BufferedReader(new FileReader(new File(source)))

    // Sample two lines
    val firstLine = reader.readLine()
    val secondLine = reader.readLine()
    reader.close()

    // Choose a good separator from candidates
    var separator = separators(0)
    if (separators.size > 1) {
      // Choose one
      separator = separators.filter(sep => {
        val split1 = firstLine.split(sep)
        val split2 = secondLine.split(sep)
        split1.size > 1 && split1.size == split2.size
      })(0)
    }

    // Determine if the column has a header
    val split1 = firstLine.split(separator).map(s => SchemaGuesser.testType(s, DataType.BOOLEAN))
    val split2 = secondLine.split(separator).map(s => SchemaGuesser.testType(s, DataType.BOOLEAN))

    val hasHeader = !split1.zip(split2).map(pair => pair._1 == pair._2).reduce((a, b) => a && b)

    val commonsCSVParser = new CommonsCSVParser
    commonsCSVParser.format = CSVFormat.DEFAULT.withDelimiter(separator).withIgnoreEmptyLines(true)
      .withAllowMissingColumnNames(true).withSkipHeaderRecord(hasHeader)

    if (separator == '|') {
      commonsCSVParser.format = commonsCSVParser.format.withQuote(0x03.toChar)
    }
    commonsCSVParser.headerInFile = hasHeader
    if (!hasHeader) {
      // Default field name
      commonsCSVParser.guessedHeader = (0 until split1.size).map(i => "field_%d".format(i)).toArray
    } else {
      commonsCSVParser.guessedHeader = firstLine.split(separator).map(_.replaceAll("[^\\d\\w_]+", "_"))
      commonsCSVParser.format = commonsCSVParser.format.withFirstRecordAsHeader()
    }
    commonsCSVParser
  }
}