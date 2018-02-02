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

import java.io.{FileOutputStream, PrintWriter}

import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.MineSingleFile.{output, pattern}
import edu.uchicago.cs.encsel.ptnmining.matching.GenRegexVisitor
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer
import org.apache.commons.lang3.StringUtils

import scala.io.Source

object MineFromColumn extends App {

  val patternMiner = new PatternMiner
  val output = new PrintWriter(new FileOutputStream("pattern_res"))

  val persist = Persistence.get
  persist.load().filter(_.dataType == DataType.STRING).foreach(column => {
    val colid = column.asInstanceOf[ColumnWrapper].id
    val pattern = patternMiner.mine(Source.fromFile(column.colFile).getLines()
      .take(100).toSeq.map(Tokenizer.tokenize(_).toSeq))

    val validator = new PatternValidator
    pattern.visit(validator)
    //    if (validator.isValid) {
    //      val subcols = SplitColumn.split(column, pattern)
    //      persist.save(subcols)
    //    }
    pattern.naming()
    val regex = new GenRegexVisitor
    pattern.visit(regex)
    output.println("%d:%s:%s".format(colid, validator.isValid, regex.get))
  })
  output.close
}

/**
  * Encode single file for test purpose
  */
object MineSingleFile extends App {
  val patternMiner = new PatternMiner

  val output = new PrintWriter(new FileOutputStream("pattern_res"))

  val pattern = patternMiner.mine(Source.fromFile("/home/harper/pattern/test").getLines()
    .take(100).filter(!StringUtils.isEmpty(_)).toList.map(Tokenizer.tokenize(_).toList))

  val validator = new PatternValidator
  pattern.visit(validator)
  pattern.naming()
  val regex = new GenRegexVisitor
  pattern.visit(regex)
  output.println(regex.get)
  output.close
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

  val unionThreshold = 50
  val seqThreshold = 50

  override def on(ptn: Pattern): Unit = {
    valid &= (ptn match {
      case PEmpty => !path.isEmpty
      case union: PUnion => union.content.size <= unionThreshold
      case seq: PSeq => seq.content.size <= seqThreshold
      case _ => true
    })
  }

  def isValid: Boolean = valid
}