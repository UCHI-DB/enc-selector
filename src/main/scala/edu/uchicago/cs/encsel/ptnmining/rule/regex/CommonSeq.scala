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

package edu.uchicago.cs.encsel.ptnmining.rule.regex

import scala.collection.mutable.ArrayBuffer


/**
  * Look for common sequences from a list of tokens
  *
  */

object CommonSeq {
  val DEFAULT_SEQ_LENGTH = 1
  // Percentage that a common sequence is not in some sentence
  // TODO: the tolerance is not supported now
  val DEFAULT_TOLERANCE = 0.1
}

class CommonSeq(val seqLength: Int = CommonSeq.DEFAULT_SEQ_LENGTH,
                val tolerance: Double = CommonSeq.DEFAULT_TOLERANCE) {

  implicit def bool2int(b: Boolean) = if (b) 1 else 0

  var positions = new ArrayBuffer[Seq[(Int, Int)]]

  /**
    * Look for common sequence in a list of lines. For implementation
    * simplicity, only the longest common seq is returned
    *
    * @param lines
    * @return common sequences
    */
  def find[T](lines: Seq[Seq[T]], equal: (T, T) => Boolean = (a: T, b: T) => {
    a.equals(b)
  }): Seq[Seq[T]] = {
    positions.clear

    val commons = new ArrayBuffer[Seq[T]]()
    commons += lines.head
    var firstLine = true
    lines.drop(1).foreach(line => {
      if (commons.nonEmpty) {
        val commonsBetween = new ArrayBuffer[Seq[(Int, Int, Int)]]

        // val commonsBetween = commons.map(between(_, line, equal))
        var mask = 0
        commons.foreach(cm => {
          val btwn = between(cm, line.drop(mask), equal)
          commonsBetween += btwn.map(x => (x._1, x._2 + mask, x._3))
          if (btwn.nonEmpty)
            mask = btwn.last._2 + btwn.last._3
        })

        // Remove positions that are no longer valid
        val emptyIndices = commonsBetween.zipWithIndex
          .filter(_._1.isEmpty).map(_._2).toSet
        emptyIndices.isEmpty match {
          case true => Unit
          case false => {
            val newpos = positions.view.map(pos => pos.zipWithIndex
              .filter(act => {
                !emptyIndices.contains(act._2)
              }).map(_._1)).force
            positions.clear
            positions ++= newpos
          }
        }
        // In this algorithm, the commons between will not overlap
        val nonOverlap = commonsBetween.filter(_.nonEmpty)
        nonOverlap.isEmpty match {
          case true => commons.clear
          case false => {
            // Split the positions
            if (firstLine) {
              // For the first and second lines
              positions += nonOverlap(0).map(cmn => (cmn._1, cmn._3))
              positions += nonOverlap(0).map(cmn => (cmn._2, cmn._3))
              firstLine = false
            } else {
              positions = positions.map(pos => {
                pos.zip(nonOverlap).flatMap(pair => {
                  val oldpos = pair._1
                  val newseps = pair._2
                  newseps.map(newsep => (oldpos._1 + newsep._1, newsep._3))
                })
              })
              positions += nonOverlap.flatten.map(item => (item._2, item._3))
            }
            commons.clear
            commons ++= positions.last.map(pos => line.slice(pos._1, pos._1 + pos._2))
          }
        }
      }
    })
    commons
  }

  /**
    * Find Common sub-sequence in two sequences
    *
    * This method will choose longer sequence for two overlapped sequences.
    *
    * @param a     the first sequence
    * @param b     the second sequence
    * @param equal equality test function
    * @return sequence of common symbols with length >= <code>sequence_length</code>.
    *         (a_start, b_start, length)
    */
  def between[T](a: Seq[T], b: Seq[T], equal: (T, T) => Boolean): Seq[(Int, Int, Int)] = {
    if (a.isEmpty || b.isEmpty)
      return Seq[(Int, Int, Int)]()
    val data = a.indices.map(i => new Array[Int](b.length))
    a.indices.foreach(i => data(i)(0) = equal(a(i), b.head))
    b.indices.foreach(i => data(0)(i) = equal(a.head, b(i)))

    val candidates = new ArrayBuffer[(Int, Int, Int)]

    for (i <- 1 until a.length;
         j <- 1 until b.length) {
      data(i)(j) = equal(a(i), b(j)) match {
        case true => data(i - 1)(j - 1) + 1
        case false => 0
      }
    }
    // Collecting results
    for (i <- a.indices;
         j <- b.indices) {
      if (data(i)(j) >= seqLength) {
        candidates += ((i - data(i)(j) + 1, j - data(i)(j) + 1, data(i)(j)))
      }
    }

    // Removing overlap
    val pha = Array.fill(a.length)(0)
    val phb = Array.fill(b.length)(0)
    val not_overlap = new ArrayBuffer[(Int, Int, Int)]
    // From long to short
    candidates.sortBy(x => (-x._3, x._1, x._2)).foreach(c => {
      val afree = !pha.slice(c._1, c._1 + c._3).toSet.exists(_ >= c._3)
      val bfree = !phb.slice(c._2, c._2 + c._3).toSet.exists(_ >= c._3)
      if (afree && bfree) {
        not_overlap += c
        (c._1 until c._1 + c._3).foreach(pha(_) = c._3)
        (c._2 until c._2 + c._3).foreach(phb(_) = c._3)
      }
    })
    not_overlap.sortBy(_._1)
  }
}

