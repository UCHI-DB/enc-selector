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
import edu.uchicago.cs.encsel.dataset.feature.classify.BlockSimilarWords.msgSize
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

    var r: Long = Math.abs(Random.nextLong()) % p
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

object BlockSimilarWords extends FeatureExtractor {
  override def featureType: String = "SimilarWords"

  override def supportFilter: Boolean = true

  val windowSize = (1 << 15) - 1

  val msgSize = (1 << 8) - 1

  val p = 3593406659l

  override def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)

    val fpr = new Fingerprint(p)

    val ratios = new mutable.ArrayBuffer[Double]()

    val buffer = new Array[Byte](windowSize)
    var size = 0
    var skipProb = 1.0 / msgSize
    do {
      if (Random.nextDouble() < skipProb) {
        input.skip(windowSize)
        size = windowSize
      } else {
        size = input.read(buffer)
        val blockRatio = scanBlock(buffer, size, fpr)
        ratios += blockRatio
      }
    }
    while (size == buffer.length)

    return Iterable(new Feature(fType, "block_ratio", ratios.sum / ratios.size))
  }

  def scanBlock(buffer: Array[Byte], size: Int, fpr: Fingerprint): Double = {
    var exists = new mutable.HashSet[Long]
    var suffixFp = Array.fill[Long](msgSize)(0l);
    var suffixLength = 0
    val lengthCounter = Array.fill[Long](msgSize + 1)(0l)
    var pointer = 0
    while (pointer < size) {
      // Look forward for the longest prefix
      var fpointer = pointer
      var fp: Long = buffer(fpointer)
      val fpsteps: Array[Long] = Array.fill[Long](msgSize)(0l)
      fpsteps(0) = fp
      while (exists.contains(fp)) {
        fpointer += 1
        fp = fpr.append(fp, fpointer - pointer, buffer(fpointer))
        fpsteps(fpointer - pointer) = fp
      }
      // Found the longest prefix and the next message is fp
      val msglen = fpointer - pointer + 1
      lengthCounter(msglen) += 1
      exists += fp
      pointer = fpointer + 1

      // Now construct all substrings introduced by the new message
      for (i <- 1 to suffixLength) {
        // Left side
        for (j <- 1 to Math.min(msgSize - 1, msglen)) { // Right side
          exists += fpr.combine(suffixFp(i), i, fpsteps(j))
        }
      }
      // Now construct new suffix

      suffixLength = Math.min(pointer, msgSize - 1)
      for (i <- 1 until Math.max(0, suffixLength - msglen)) {
        suffixFp(i) = fpr.divide(fp - fpsteps(msglen - i), msglen - i)
      }
    }
    return lengthCounter.zipWithIndex.map(p => p._1 * p._2).sum.toDouble / size
  }
}


class Fingerprint(p: Long) {

  var r: Long = Math.abs(Random.nextLong()) % p
  var rp = new Array[Long](msgSize);
  rp(0) = 1
  for (i <- 1 until msgSize) {
    rp(i) = (rp(i - 1) * r) % p
  }

  def get(s: String): Long = {
    s.zipWithIndex.map(pair => pair._1 * rp(pair._2) % p).reduce((x1, x2) => (x1 + x2) % p)
  }

  def append(fp: Long, length: Int, char: Byte) = {
    (fp + rp(length) * char) % p
  }

  def combine(lfp: Long, llen: Int, rfp: Long): Long = {
    (lfp + (rp(llen) * rfp) % p) % p
  }

  def divide(fp: Long, pow: Int): Long = {
    // TODO
    throw new UnsupportedOperationException()
  }
}