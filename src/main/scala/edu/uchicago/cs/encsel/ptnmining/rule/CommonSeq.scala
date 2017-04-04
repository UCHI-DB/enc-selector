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

package edu.uchicago.cs.encsel.ptnmining.rule

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by Hao Jiang on 3/14/17.
  */


class CommonSeq {

  val sequence_length = 1
  // Percentage that a common sequence is not in some sentence
  // TODO: the tolerance is not supported now
  val tolerance = 0.1

  implicit def bool2int(b: Boolean) = if (b) 1 else 0

  var positions = new ArrayBuffer[Seq[(Int, Int)]]

  /**
    * Look for common sequence in a list of lines. For implementation
    * simplicity, only the longest common seq is returned
    *
    * @param lines
    * @return common sequences
    */
  def find[T](lines: Seq[Seq[T]], equal: (T, T) => Boolean): Seq[Seq[T]] = {
    positions.clear

    val commons = new ArrayBuffer[Seq[T]]()
    commons += lines.head

    lines.drop(1).foreach(line => {
      if (commons.nonEmpty) {
        val commonsBetween = commons.map(between(_, line, equal))

        // Remove positions that are no longer valid
        val emptyIndices = commonsBetween.zipWithIndex
          .filter(_._1.isEmpty).map(_._2).toSet
        positions = positions.map(pos => pos.zipWithIndex
          .filter(act => {
            !emptyIndices.contains(act._2)
          }).map(_._1))

        val nonOverlap = commons.length match {
          case 1 => commonsBetween
          case _ => {
            // Remove overlapped items
            val withIndex = commonsBetween.zipWithIndex.map(outer => {
              outer._1.zipWithIndex.map(inner => {
                (inner._1, outer._2, inner._2)
              })
            }).flatten

            val placeholder = Array.fill(line.length)(0)
            val filtered = withIndex.sortBy(-_._1._3).filter(item => {
              val piece = item._1
              if (placeholder.slice(piece._2, piece._2 + piece._3).sum == 0) {
                (piece._2 until piece._2 + piece._3).foreach(placeholder(_) = piece._3)
                true
              } else {
                false
              }
            })

            val grouped = filtered.groupBy(_._2).toSeq.sortBy(_._1).map(i => (i._1, i._2.map(j => (j._1, j._3))))

            val nolp = new ArrayBuffer[Seq[(Int, Int, Int)]]
            grouped.foreach(item => {
              while (nolp.length < item._1 - 1) {
                nolp += Seq.empty[(Int, Int, Int)]
              }
              nolp += item._2.sortBy(_._2).map(_._1)
            })
            nolp
          }
        }

        // Split the positions
        if (positions.isEmpty) {
          // For the first and second lines
          positions += nonOverlap(0).map(cmn => (cmn._1, cmn._3))
          positions += nonOverlap(0).map(cmn => (cmn._2, cmn._3))
        } else {
          positions = positions.map(pos => {
            pos.zip(nonOverlap).map(pair => {
              val oldpos = pair._1
              val newseps = pair._2
              newseps.map(newsep => (oldpos._1 + newsep._1, newsep._3))
            }).flatten
          })
          positions += nonOverlap.flatten.map(item => (item._2, item._3))
        }
        commons.clear
        commons ++= positions.last.map(pos => line.slice(pos._1, pos._1 + pos._2))
      } else {
        commons.clear()
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
    for (i <- 0 until a.length;
         j <- 0 until b.length) {
      if (data(i)(j) >= sequence_length) {
        candidates += ((i - data(i)(j) + 1, j - data(i)(j) + 1, data(i)(j)))
      }
    }

    // Removing overlap
    val pha = Array.fill(a.length)(0)
    val phb = Array.fill(b.length)(0)
    val not_overlap = new ArrayBuffer[(Int, Int, Int)]
    // From long to short
    candidates.sortBy(-_._3).foreach(c => {
      val afree = pha.slice(c._1, c._1 + c._3).toSet.filter(_ >= c._3).isEmpty
      val bfree = phb.slice(c._2, c._2 + c._3).toSet.filter(_ >= c._3).isEmpty
      if (afree && bfree) {
        not_overlap += c
        (c._1 until c._1 + c._3).foreach(pha(_) = c._3)
        (c._2 until c._2 + c._3).foreach(phb(_) = c._3)
      }
    })
    not_overlap.sortBy(_._1)
  }
}
