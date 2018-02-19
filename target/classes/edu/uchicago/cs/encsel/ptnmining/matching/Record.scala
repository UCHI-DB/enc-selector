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

package edu.uchicago.cs.encsel.ptnmining.matching

import edu.uchicago.cs.encsel.ptnmining.parser.Token

import scala.collection.mutable

/**
  * Mapping between pattern node and token
  */
class Record {
  val values: mutable.HashMap[String, Token] = new mutable.HashMap[String, Token]

  val choices: mutable.HashMap[String, (Int, Int)] = new mutable.HashMap[String, (Int, Int)]

  // The delta towards min value of range
  val rangeDeltas: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]

  def add(name: String, value: Token) = {
    values += ((name, value))
  }

  def get(name: String): Option[Token] = values.get(name)
}

