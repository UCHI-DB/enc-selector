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

package edu.uchicago.cs.encsel.dataset


import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.model._
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper

object EncodeAllColumns extends App {
  Persistence.get.load.foreach(col => {
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          ParquetWriterHelper.singleColumnString(col.colFile, e)
        )
      }
      case DataType.LONG => {
        LongEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          ParquetWriterHelper.singleColumnLong(col.colFile, e)
        )
      }
      case DataType.INTEGER => {
        IntEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          ParquetWriterHelper.singleColumnInt(col.colFile, e)
        )
      }
      case DataType.FLOAT => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          ParquetWriterHelper.singleColumnFloat(col.colFile, e)
        )
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          ParquetWriterHelper.singleColumnDouble(col.colFile, e)
        )
      }
      case DataType.BOOLEAN => {}
    }
  })
}
