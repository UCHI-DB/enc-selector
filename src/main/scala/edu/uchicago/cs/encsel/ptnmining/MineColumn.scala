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

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.matching.RegexMatcher
import edu.uchicago.cs.encsel.ptnmining.parser.Tokenizer
import edu.uchicago.cs.encsel.util.FileUtils
import org.apache.commons.lang3.StringUtils

import scala.io.Source
import scala.util.Random

object MineColumn {
  val patternMiner = new PatternMiner
  val matcher = RegexMatcher

  def patternFromFile(file: URI): Pattern = {
    val lines = Source.fromFile(file).getLines().map(_.trim).filter(_.nonEmpty).take(5000).filter(p => {
      Random.nextDouble() <= 0.1
    })
    //    val tail = lines.takeRight(100)
    //    val both = head ++ tail
    val pattern = patternMiner.mine(lines.map(Tokenizer.tokenize(_).toSeq).toSeq)
    pattern.naming()

    pattern
  }

  def numChildren(pattern: Pattern): Int = {
    val validator = new PatternValidator
    pattern.visit(validator)
    validator.isValid match {
      case false => 0
      case true => {
        (pattern match {
          case seq: PSeq => {
            seq.content.flatMap(_ match {
              case union: PUnion => Some(union)
              case any: PAny => Some(any)
              case _ => None
            })
          }
          case _ => Seq()
        }).size
      }
    }
  }

  def split(column: Column, pattern: Pattern): Seq[Column] = {
    if (column.dataType != DataType.STRING)
      throw new IllegalArgumentException()
    pattern.naming()
    pattern match {
      case seq: PSeq => {
        val colPatterns = seq.content.flatMap(_ match {
          case union: PUnion => Some(union)
          case any: PAny => Some(any)
          case _ => None
        })
        val childColumns = colPatterns.zipWithIndex.map(pi => {
          val col = new Column(null, pi._2, String.valueOf(pi._2), typeof(pi._1))
          col.colFile = FileUtils.addExtension(column.colFile, pi._2.toString)
          col.parent = column
          col
        }).toList
        // columns for unmatched lines
        val unmatchCol = new Column(null, -1, "unmatch", DataType.STRING)
        unmatchCol.colFile = FileUtils.addExtension(column.colFile, "unmatch")
        unmatchCol.parent = column

        val outputs = childColumns.map(col => new PrintWriter(new FileOutputStream(new File(col.colFile))))
        val umoutput = new PrintWriter(new FileOutputStream(new File(unmatchCol.colFile)))

        val source = Source.fromFile(column.colFile)
        try {
          source.getLines().map(_.trim).foreach(line => {
            if (!StringUtils.isEmpty(line)) {
              // Match the line against pattern
              val matched = matcher.matchon(pattern, line)
              if (matched.isDefined) {
                colPatterns.indices.foreach(i => {
                  val ptn = colPatterns(i)
                  ptn match {
                    case iany: PIntAny => {
                      val value = matched.get.get(ptn.name)
                      if (value.isEmpty) {
                        outputs(i).println("")
                      } else
                        outputs(i).println(
                          BigInt(matched.get.get(ptn.name), if (iany.hasHex) 16 else 10).toString
                        )
                    }
                    case _ => {
                      outputs(i).println(matched.get.get(ptn.name))
                    }
                  }

                })
              } else {
                // Not match, write to unmatch
                umoutput.println(line)
              }
            } else {
              // Output empty line for empty line
              outputs.foreach(_.println(""))
            }
          })
          childColumns :+ unmatchCol
        } finally {
          source.close
          outputs.foreach(_.close)
          umoutput.close
        }
      }
      case _ => Seq()
    }
  }

  def typeof(pattern: Pattern): DataType = {
    pattern match {
      case iany: PIntAny => {
        if (iany.maxLength < 0 || (iany.hasHex && iany.maxLength > 8) || (!iany.hasHex && iany.maxLength > 9))
          DataType.LONG
        else
          DataType.INTEGER
      }
      case dany: PDoubleAny => DataType.DOUBLE
      case union: PUnion => {
        if (union.content.size == 2 && union.content.contains(PEmpty)) {
          val remain = union.content.filter(_ != PEmpty).head
          typeof(remain)
        }
        else
          DataType.STRING
      }
      case _ => DataType.STRING
    }
  }

  def splitDouble(column: Column): Seq[Column] = {
    if (column.dataType != DataType.DOUBLE)
      throw new IllegalArgumentException()

    val outputs = (0 to 1).map(pi => FileUtils.addExtension(column.colFile, pi.toString)).toList
      .map(col => new PrintWriter(new FileOutputStream(new File(col))))
    val types = Array.fill(2)(DataType.INTEGER)

    val source = Source.fromFile(column.colFile)
    try {
      source.getLines().map(_.trim).foreach(line => {
        if (!StringUtils.isEmpty(line)) {
          // Extract pieces
          val split = line.split("\\.")
          val data = (split.length match {
            case 2 => {
              (split(0), split(1))
            }
            case 1 => {
              (split(0), "0")
            }
            case _ => throw new IllegalArgumentException
          }).productIterator.toList
          // Update type
          for (i <- 0 to 1) {
            if (types(i) == DataType.INTEGER) {
              types(i) = try {
                data(i).toString.toInt
                DataType.INTEGER
              } catch {
                case e: NumberFormatException =>
                  DataType.LONG
              }
            }
          }
          outputs(0).println(data(0))
          outputs(1).println(data(1))
        } else {
          // Output empty line for empty line
          outputs.foreach(_.println(""))
        }
      })
      val childColumns = (0 to 1).map(pi => {
        val col = new Column(null, pi, String.valueOf(pi), types(pi))
        col.colFile = FileUtils.addExtension(column.colFile, pi.toString)
        col.parent = column
        col
      }).toList

      childColumns

    } finally {
      source.close
      outputs.foreach(_.close)
    }
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
  val seqThreshold = 15

  override def on(ptn: Pattern): Unit = {
    valid &= (ptn match {
      case union: PUnion => !path.isEmpty && union.content.size <= unionThreshold
      case seq: PSeq => seq.content.filter(!_.isInstanceOf[PToken]).size <= seqThreshold
      case _ => !path.isEmpty
    })
  }

  def isValid: Boolean = valid
}