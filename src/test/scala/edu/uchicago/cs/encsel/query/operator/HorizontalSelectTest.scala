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

package edu.uchicago.cs.encsel.query.operator

import java.io.File

import edu.uchicago.cs.encsel.query.HColumnPredicate
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema
import org.apache.parquet.io.api.Binary
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class HorizontalSelectTest {

  @Test
  def testSelectInPredicate: Unit = {

    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new HColumnPredicate((data) => data.toString.toInt >= 90, 0)
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(0, 1, 2))

    assertEquals(11, temptable.getRecords.size)

    (0 to 10).foreach(i => {
      var record = temptable.getRecords()(i)
      assertEquals(3, record.getData.length)
      assertEquals(i + 90, record.getData()(0).toString.toInt)
    })
  }

  @Test
  def testSelectNotInPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new HColumnPredicate((data) => data.toString.toInt >= 90, 0)
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    assertEquals(11, temptable.getRecords.size)

    (0 to 10).foreach(i => {
      var record = temptable.getRecords()(i)
      assertEquals(4, record.getData.length)
      assertEquals("Customer#000000%03d".format(i + 90), record.getData()(0).asInstanceOf[Binary].toStringUsingUTF8)
    })
  }
}
