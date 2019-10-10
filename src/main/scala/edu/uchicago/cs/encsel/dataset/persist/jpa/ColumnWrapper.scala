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

package edu.uchicago.cs.encsel.dataset.persist.jpa

import edu.uchicago.cs.encsel.dataset.column.Column

class ColumnWrapper extends Column {

  var id: Int = 0
  var parentWrapper: ColumnWrapper = null

  override def parent: Column = parentWrapper

  override def parent_=(col: Column) = parentWrapper = col
}

object ColumnWrapper {
  implicit def fromColumn(col: Column): ColumnWrapper = {
    if (null == col)
      return null
    if (col.isInstanceOf[ColumnWrapper])
      return col.asInstanceOf[ColumnWrapper]
    val wrapper = new ColumnWrapper
    wrapper.colFile = col.colFile
    wrapper.colName = col.colName
    wrapper.colIndex = col.colIndex
    wrapper.dataType = col.dataType
    wrapper.origin = col.origin
    wrapper.parentWrapper = fromColumn(col.parent)
    wrapper.features = col.features

    wrapper
  }
}

