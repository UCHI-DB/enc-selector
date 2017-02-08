/**
 * *****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 * *****************************************************************************
 */
package edu.uchicago.cs.encsel.parser.json

import scala.annotation.migration
import scala.collection.JavaConversions.asScalaSet

import com.google.gson.JsonObject

import edu.uchicago.cs.encsel.parser.DefaultRecord
import edu.uchicago.cs.encsel.parser.Parser
import edu.uchicago.cs.encsel.parser.Record
import edu.uchicago.cs.encsel.schema.Schema
import scala.io.Source
import java.net.URI

/**
 * This Parser parse per-line json object format, which
 * is common when large files are being processed
 */
class LineJsonParser extends Parser {

  headerInline = true
  var jsonParser = new com.google.gson.JsonParser

  override def parseLine(line: String): Record = {
    var jsonObject = jsonParser.parse(line).getAsJsonObject
    if (schema != null) {
      new DefaultRecord(schema.columns.map(f => jsonField(jsonObject, f._2)).toArray)
    } else { // Read keys and order
      new DefaultRecord(guessedHeader.map(key => jsonField(jsonObject, key)).toArray)
    }
  }

  protected override def guessHeader(line: String): Unit = {
    var jsonObject = jsonParser.parse(line).getAsJsonObject
    guessedHeader = jsonObject.entrySet().map(f => f.getKey).toList.sorted.toArray
  }

  protected def jsonField(jsonObject: JsonObject, key: String): String = {
    if (jsonObject.has(key))
      jsonObject.get(key) match {
        case x if x.isJsonPrimitive() => x.getAsString
        case x if x.isJsonArray() => x.toString()
        case x if x.isJsonNull() => ""
        case x if x.isJsonObject() => x.toString()
      }
    else ""
  }

}