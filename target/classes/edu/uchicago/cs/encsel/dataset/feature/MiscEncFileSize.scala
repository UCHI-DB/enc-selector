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

package edu.uchicago.cs.encsel.dataset.feature

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.encoding.{Encoding, EncodingException}
import org.apache.commons.lang.StringUtils
import org.apache.parquet.hadoop.ParquetWriter

class MiscEncFileSize(enc: Encoding) extends FeatureExtractor {
  override def featureType = "EncFileSize"

  override def supportFilter = false

  override def extract(input: Column, prefix: String) = {
    try {
      val enctype = enc.enctype(input.dataType)
      val outputFile = ParquetWriterHelper.genOutput(input.colFile, enctype);
      try {
        enc.encode(input, outputFile.toURI)
        Iterable(new Feature(featureType, "%s_file_size".format(enctype), outputFile.length))
      } catch {
        case e: EncodingException => {
          // Error occurred when doing encoding
          Iterable(new Feature(featureType, "%s_file_size".format(enctype), -1))
        }
      }

    } catch {
      case e: UnsupportedOperationException => Iterable[Feature]()
    }
  }
}
