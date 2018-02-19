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

package edu.uchicago.cs.encsel.query

import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

object DataUtils {
  def readValue(column: ColumnReaderImpl): Any = {
    column.getDescriptor.getType match {
      case PrimitiveTypeName.DOUBLE => column.getDouble
      case PrimitiveTypeName.BINARY => column.getBinary
      case PrimitiveTypeName.INT32 => column.getInteger
      case PrimitiveTypeName.INT64 => column.getLong
      case PrimitiveTypeName.FLOAT => column.getFloat
      case PrimitiveTypeName.BOOLEAN => column.getBoolean
      case _ => throw new IllegalArgumentException
    }
  }
}
