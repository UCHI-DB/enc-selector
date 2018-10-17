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

package edu.uchicago.cs.encsel.dataset.feature.classify

import java.io.InputStream

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor}

import scala.collection.mutable
import scala.util.Random

object SimilarWords extends FeatureExtractor {

  def featureType = "SimilarWords"

  def supportFilter: Boolean = true

  val windowSize = (1 << 15) - 1
  val msgSize = (1 << 8) - 1

  val p = 3593406659l

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)

    // Map a Karp-Rabin fingerprint to a position in the queue
    var lookup = new mutable.HashSet[Long]()
    var queueStart = 0
    var queue = new mutable.Queue[(Long, Long)]()

    var r = Math.abs(Random.nextLong()) % p
    var rp = new Array[Long](msgSize);
    rp(0) = 1
    for (i <- 1 until msgSize) {
      rp(i) = (rp(i - 1) * r) % p
    }

    var lengthCounter = Array.fill[Long](msgSize + 1)(0)

    var pointer = 0l
    var fingerprint = 0l
    var msglen = 0

    val buffer = new Array[Byte](10000)

    var size = 0
    do {
      size = input.read(buffer)
      for (i <- 0 until size) {
        pointer += 1
        // Remove the substring that is already out of window
        if (!queue.isEmpty && queue.front._2 <= pointer - windowSize) {
          val outOfWindow = queue.dequeue()
          lookup.remove(outOfWindow._1)
        }

        // Compute the fingerprint for the string
        fingerprint = (fingerprint + buffer(i) * rp(msglen)) % p
        msglen += 1

        if (msglen == msgSize) {
          if (!lookup.contains(fingerprint)) {
            lookup.add(fingerprint)
            queue.enqueue((pointer - msgSize, fingerprint))

            lengthCounter(msglen) += 1
          }
          msglen = 0
          fingerprint = 0l
        } else {
          if (!lookup.contains(fingerprint)) {
            // If does not contain the string
            lookup.add(fingerprint)
            queue.enqueue((pointer - msglen, fingerprint))

            lengthCounter(msglen) += 1

            msglen = 0
            fingerprint = 0l
          }
        }
      }
    }
    while (size == buffer.length)

    val kmSum = lengthCounter.zipWithIndex.map(pair => pair._1 * pair._2).sum.toDouble

    return Iterable(
      new Feature(fType, "ratio", kmSum / pointer),
      new Feature(fType, "value", kmSum)
    )
  }
}

object AccurateSimilarWords extends FeatureExtractor {
  override def featureType: String = "AccurateSimilarWords"

  override def supportFilter: Boolean = true

  val windowSize = (1 << 15) - 1

  val msgSize = (1 << 8) - 1

  val p = 3593406659l

  override def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)

    var r = Math.abs(Random.nextLong()) % p
    var rp = new Array[Long](msgSize);
    rp(0) = 1
    for (i <- 1 until msgSize) {
      rp(i) = (rp(i - 1) * r) % p
    }

    val substrs = new mutable.HashSet[Long];
    var suffixs = new mutable.HashMap[Long, Int];

    val lengthCounter = Array.fill[Long](msgSize + 1)(0)
    val buffer = new Array[Byte](windowSize)

    var pointer = 0
    var size = 0
    do {
      size = input.read(buffer)
      for (i <- 0 until size) {
        // Maintain the dictionary
        val char = buffer(i).asInstanceOf[Long];
        val newsuffix = new mutable.HashMap[Long, Int];
        newsuffix += ((char, 1));
        suffixs.foreach(suffix => {
          if (suffix._2 < msgSize) {
            newsuffix += ((suffix._1 + char * rp(suffix._1.toInt), suffix._2 + 1))
          }
        })
        suffixs = newsuffix
        substrs ++= suffixs.keySet

        // Look for the longest prefix starting at (i)
        for(msg<- 1 to msgSize) {

        }

      }
    }
    while (size == buffer.length)

    return Iterable()
  }
}