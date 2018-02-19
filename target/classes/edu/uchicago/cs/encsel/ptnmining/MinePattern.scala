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

import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer

import scala.collection.mutable
import scala.io.Source

/**
  * Mine Pattern from string type data
  */
object MinePattern extends App {

  val histogram = new mutable.HashMap[Int, Int]
  val miner = new PatternMiner

  Persistence.get.lookup(DataType.STRING).foreach(column => {

    val rows = Source.fromFile(column.colFile).getLines().slice(0, 2000).toSeq

    val train = rows.slice(0, 1000)
    val test = rows.slice(1000, 2000)

    val pattern = miner.mine(train.map(Tokenizer.tokenize(_).toSeq))

    var failed = 0

    test.map(Tokenizer.tokenize).foreach(tokens => {
      pattern.matchon(tokens.toSeq) match {
        case None => {
          failed += 1
        }
        case Some(record) => {
        }
      }
    })
    val failure = failed.toDouble / 1000
    val index = (failure / 0.1).toInt

    histogram.put(index, histogram.getOrElseUpdate(index, 0) + 1)

  })
  println(histogram)
}
