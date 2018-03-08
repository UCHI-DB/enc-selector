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

import java.io.{BufferedReader, File, FileReader}
import javax.persistence.NoResultException

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.encsel.ptnmining.MineColumn._
import edu.uchicago.cs.encsel.ptnmining.compose.PatternComposer
import edu.uchicago.cs.encsel.ptnmining.persist.PatternWrapper
import edu.uchicago.cs.encsel.util.FileUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

/**
  * This tool is used to mine from columns that were missing before
  * Constant: 19535 is the largest id for original columns
  */
object MineFixer extends App {

  val persist = new JPAPersistence
  booleanAsString

  def tooManyUnmatch: Unit = {
    val start = args.length match {
      case 0 => 0
      case _ => args(0).toInt
    }

    val loadcols = persist.em.createQuery("SELECT p FROM Column p WHERE p.dataType = :dt AND EXISTS (SELECT c FROM Column c WHERE c.parentWrapper = p)", classOf[ColumnWrapper])
      .setParameter("dt", DataType.STRING).getResultList

    loadcols.asScala.foreach(column => {
      // Check the unmatched file size, if non-zero, perform a rematch
      getUnmatch(column) match {
        case Some(unmatch) => {
          val numUnmatch = FileUtils.numLine(unmatch.colFile)
          if (numUnmatch != 0) {
            println("[Info ] column %d has error %d, regenerating".format(column.id, numUnmatch))
            mineColumn(column)
          }
        }
        case _ => {
          println("[Error] column %d has no unmatch".format(column.id))
        }
      }
    })
  }


  // Some (most) integer columns was encoded as long and will not be well encoded,
  // Find them and fix them
  def retypeIntColumnAndEncode: Unit = {
    val potColumns = persist.em.createQuery("SELECT c FROM Column c WHERE c.dataType = :dt AND c.parentWrapper IS NOT NULL", classOf[ColumnWrapper])
      .setParameter("dt", DataType.LONG).getResultList.asScala

    potColumns.foreach(col => {
      val source = Source.fromFile(col.colFile)
      val hasLong = source.getLines().filter(!_.isEmpty).exists(line => {
        val bint = BigInt(line)
        bint.toInt != bint.toLong
      })
      source.close()
      if (!hasLong) {
        println(col.id)
        col.dataType = DataType.INTEGER
        col.replaceFeatures(ParquetEncFileSize.extract(col))
        persist.save(Seq(col))
      }
    })
  }

  def booleanAsString: Unit = {
    val colsHasPattern = persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper IS NULL AND EXISTS (SELECT ptn FROM Pattern ptn WHERE ptn.column = c)", classOf[ColumnWrapper]).getResultList.asScala

    colsHasPattern.foreach(col => {
      val pattern = new PatternComposer(getPattern(col))
      val children = getChildren(col).filter(_.colIndex != -1)
      val checkFailed = pattern.booleanColumns.filter(i => children(i).dataType != DataType.BOOLEAN)
      if (checkFailed.nonEmpty) {
        //        println("Mismatch found and fixing : %d".format(col.id))
        /*
        // Replace a STRING column with BOOLEAN column
        checkFailed.foreach(i => {
          // 1. Update type
          children(i).dataType = DataType.BOOLEAN
          persist.save(Iterable(children(i)))
          // 2. Update column content


        })
        */
        checkFailed.foreach(i => {
          val fileReader = new BufferedReader(new FileReader(new File(children(i).colFile)))
          var line = fileReader.readLine()
          val charset = new mutable.HashSet[Char]
          var keep = (line != null && line.size <= 1)
          while (keep) {
            charset ++= line.toSet
            line = fileReader.readLine()
            keep = (line != null) && (line.size <= 1) && charset.size <= 1
          }
          fileReader.close()
          if ((line != null && line.size > 1) || charset.size > 1) {
            println("Data cannot be fixed : %d".format(col.id))
          }
        })
      }
    })
  }


  def getChildren(col: Column): Seq[Column] = {
    persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper = :p ORDER BY c.colIndex", classOf[ColumnWrapper])
      .setParameter("p", col).getResultList().asScala
  }

  def getPattern(col: Column): String = {
    persist.em.createQuery("SELECT p FROM Pattern p WHERE p.column = :c", classOf[PatternWrapper])
      .setParameter("c", col).getSingleResult.pattern
  }

  def getUnmatch(col: Column): Option[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent AND c.colIndex = :idx"
    try {
      Some(persist.em.createQuery(sql, classOf[ColumnWrapper])
        .setParameter("parent", col)
        .setParameter("idx", -1)
        .getSingleResult)
    } catch {
      case e: NoResultException => None
    }
  }
}
