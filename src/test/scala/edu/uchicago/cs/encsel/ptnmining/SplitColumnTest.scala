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

package edu.uchicago.cs.encsel.ptnmining

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.parser.{TSymbol, TWord}
import org.junit.Test
import org.junit.Assert._
import scala.io.Source

class SplitColumnTest {

  @Test
  def testSplit: Unit = {
    val col = new Column(null, 1, "sample", DataType.STRING)
    col.colFile = new File("src/test/resource/colsplit/column").toURI

    val pattern = PSeq.collect(
      new PToken(new TWord("MIR")),
      new PToken(new TSymbol("-")),
      new PIntAny(6),
      new PToken(new TSymbol("-")),
      new PWordAny(3),
      new PToken(new TSymbol("-")),
      new PIntAny(4)
    )
    val subcolumns = SplitColumn.split(col, pattern)

    assertArrayEquals(Array[AnyRef]("301401", "228104", "", "323421", "243242", "", "423432"),
      Source.fromFile("src/test/resource/colsplit/column.0").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("KWR", "KKP", "", "WOP", "DMN", "", "OOP"),
      Source.fromFile("src/test/resource/colsplit/column.1").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("3219", "5014", "", "4234", "4223", "", "1234"),
      Source.fromFile("src/test/resource/colsplit/column.2").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("WORKHARD","WORKHARD","WORKWORKWORKHARD"),
      Source.fromFile("src/test/resource/colsplit/column.unmatch").getLines().toArray[AnyRef])
  }
}
