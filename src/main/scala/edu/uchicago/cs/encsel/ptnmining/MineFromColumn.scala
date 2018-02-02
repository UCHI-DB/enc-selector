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

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.matching.GenRegexVisitor
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer
import org.apache.commons.lang3.StringUtils

import scala.io.Source

object MineFromColumn extends App {

  val patternMiner = new PatternMiner

  mineAllFiles

  def mineAllFiles: Unit = {
    val output = new PrintWriter(new FileOutputStream("pattern_res"))

    val persist = Persistence.get
    persist.load().filter(_.dataType == DataType.STRING).foreach(column => {
      val colid = column.asInstanceOf[ColumnWrapper].id
      val pattern = patternFromFile(column.colFile)
      val valid = validate(pattern)
      if (valid) {
        val subcols = SplitColumn.split(column, pattern)
        if (!subcols.isEmpty)
          persist.save(subcols)
      }
      val regex = new GenRegexVisitor
      pattern.visit(regex)
      output.println("%d:%s:%s".format(colid, valid, regex.get))
    })
    output.close
  }

  def mineSingleFile: Unit = {
    val file = new File("/local/hajiang/./columns/columner3524782481115062568/SERIALNUMBER_97529673794382523077.tmp").toURI
    val pattern = patternFromFile(file)
    val valid = validate(pattern)
    val regex = new GenRegexVisitor
    pattern.visit(regex)
    println("%s:%s".format(regex.get, valid))
  }

  def patternFromFile(file: URI): Pattern = {
    val lines = Source.fromFile(file).getLines().filter(!_.trim.isEmpty).toIterable
    val head = lines.take(500)
    //    val tail = lines.takeRight(100)
    //    val both = head ++ tail
    val pattern = patternMiner.mine(head.map(Tokenizer.tokenize(_).toSeq).toSeq)
    pattern.naming()

    pattern
  }

  def validate(pattern: Pattern): Boolean = {
    val validator = new PatternValidator
    pattern.visit(validator)
    validator.isValid
  }
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
      case union: PUnion => !path.isEmpty && union.content.size <= unionThreshold
      case seq: PSeq => seq.content.size <= seqThreshold
      case _ => !path.isEmpty
    })
  }

  def isValid: Boolean = valid
}