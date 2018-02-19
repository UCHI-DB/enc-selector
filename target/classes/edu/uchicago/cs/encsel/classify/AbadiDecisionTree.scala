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

package edu.uchicago.cs.encsel.classify

import edu.uchicago.cs.encsel.dataset.feature.Feature
import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.model.{DataType, IntEncoding, StringEncoding}
import org.slf4j.LoggerFactory

class AbadiDecisionTree {

  def classifyInt(arl: Feature, card: Feature, sort: Feature): IntEncoding = {
    if (arl.value >= 4) {
      return IntEncoding.RLE;
    }
    if (card.value >= 50000) {
      return IntEncoding.PLAIN;
    } else if (card.value > 50) {
      return IntEncoding.DICT;
    } else {
      return IntEncoding.BITVECTOR;
    }
  }

}

object AbadiDecisionTree extends App {

  val store = Persistence.get

  var counter = 0;
  var success = 0;

  val decTree = new AbadiDecisionTree

  store.load().foreach(col => {
    val avl = col.findFeature("AvgRunLength", "value")
    val card = col.findFeature("Distinct", "count")
    val sort = col.findFeature("Sortness", "ivpair_500")

    col.dataType match {
      case DataType.INTEGER => {
        if (avl.isDefined && card.isDefined) {
          val encodedSize = col.findFeatures("EncFileSize").toList
          val validSize = encodedSize.filter(_.value >= 0)
          if (validSize.isEmpty) {
            LoggerFactory.getLogger(getClass).warn("Invalid context for column:%s".format(col.colFile))
          } else {
            val smallestSize = validSize.minBy(_.value)
            counter += 1
            val treeDecision = decTree.classifyInt(avl.get, card.get, null)

            if ("%s_file_size".format(treeDecision.name()).equals(smallestSize.name)) {
              success += 1
            }
          }
        }
      }
      case _ => {

      }
    }
  })

  System.out.println("Success Rate: %d/%d".format(success, counter))
}
