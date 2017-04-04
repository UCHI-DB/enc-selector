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
 */

package edu.uchicago.cs.encsel.ptnmining.parser

import java.io.StringReader

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by harper on 3/25/17.
  */
object Tokenizer {
  def tokenize(line: String): Iterator[Token] = {
    val lexer = new Lexer(new StringReader(line))

    val tokens = new mutable.Stack[Token]

    val find_match: TPara => Unit = (right: TPara) => {
      if (tokens.filter(t => t.isInstanceOf[TPara] && t.asInstanceOf[TPara].matches(right)).nonEmpty) {
        val buffer = new ArrayBuffer[Token]
        while (!tokens.top.isInstanceOf[TPara] || !tokens.top.asInstanceOf[TPara].matches(right)) {
          buffer.insert(0, tokens.pop)
        }
        // Pop up the left pair
        tokens.pop
        tokens.push(new TGroup(right.paraType, buffer))
      }
    }

    var nextsym = lexer.scan()
    while (nextsym != null) {
      // For parenthetical symbols, look for pairs
      nextsym match {
        case para: TPara => {
          if (!para.left) {
            find_match(para)
          }
          else {
            tokens.push(nextsym)
          }
        }
        case _ => {
          tokens.push(nextsym)
        }
      }
      nextsym = lexer.scan()
    }

    tokens.reverseIterator
  }
}