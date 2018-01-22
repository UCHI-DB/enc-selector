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

package edu.uchicago.cs.encsel.ptnmining

import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.genregex.GenRegexVisitor
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer

import scala.io.Source

object MineFromColumn {

  val patternMiner = new PatternMiner

  Persistence.get.load().filter(_.dataType == DataType.STRING)
    .foreach(column => {
      val colid = column.asInstanceOf[ColumnWrapper].id
      val pattern = patternMiner.mine(Source.fromFile(column.colFile).getLines()
        .take(100).toSeq.map(Tokenizer.tokenize(_).toSeq))

      val validator = new PatternValidator
      pattern.visit(validator)
      if (validator.isValid) {
        pattern.naming()
        val regex = new GenRegexVisitor
        pattern.visit(regex)
        println("%d:%s".format(colid, regex.history.get(pattern.name).getOrElse("")))
      }
    })
}

/**
  * There are currently several requirements to the pattern
  * 1. No too large unions
  * 2. No too long sequences
  *
  * Note: these rules are temporary and subject to change
  */
class PatternValidator extends PatternVisitor {

  var valid = true

  val unionThreshold = 10
  val seqThreshold = 10

  override def on(ptn: Pattern): Unit = {
    ptn match {
      case union: PUnion => {
        valid &= union.content.size <= unionThreshold
      }
      case seq: PSeq => {
        valid &= seq.content.size <= seqThreshold
      }
      case _ => {}
    }
  }

  def isValid: Boolean = valid
}