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

package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining.{PIntAny, PToken, PWordAny, Pattern}

/**
  * This rule looks at top-level tokens and decide whether to upgrade them into <code>PAny</code>
  */
class GeneralizeTokenRule extends RewriteRule {

  override protected def condition(ptn: Pattern): Boolean = {
    path.size == 2 && ptn.isInstanceOf[PToken] && {
      val ptoken = ptn.asInstanceOf[PToken].token
      ptoken.isInstanceOf[TInt] || ptoken.isInstanceOf[TWord]
    }
  }

  override protected def update(ptn: Pattern): Pattern = {
    val pt = ptn.asInstanceOf[PToken]
    pt.token match {
      case int: TInt => {
        happen()
        new PIntAny(int.numChar, -1)
      }
      case word: TWord => {
        happen()
        new PWordAny(word.numChar, -1)
      }
      case _ => ptn
    }
  }
}
