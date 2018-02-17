package edu.uchicago.cs.encsel.report

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DatasetCategorier extends App {

  val persist = new JPAPersistence

  val cols = persist.em.createQuery("SELECT col FROM Column col WHERE col.parentWrapper IS NULL", classOf[ColumnWrapper])
    .getResultList.asScala

  val map = new mutable.HashMap[String, mutable.Buffer[Column]]
  val remain = new mutable.HashSet[String]

  cols.foreach(col => {
    if (col.origin.toString.contains("argonne")) {
      map.getOrElse("NationalLab", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("uci")) {
      map.getOrElse("Machine Learning", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("gis")) {
      map.getOrElse("GIS", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("bike")) {
      map.getOrElse("GIS", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("bike")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("taxi")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("911")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("311")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("311")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("tree")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("tax")) {
      map.getOrElse("Financial", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("payment")) {
      map.getOrElse("Financial", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("permit")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("fire")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("vem")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("parking")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("mv")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("pv")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("speed")) {
      map.getOrElse("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("yelp")) {
      map.getOrElse("Social Network", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("dp")) {
      map.getOrElse("Government", new ArrayBuffer[Column]) += col
    } else {
      map.getOrElse("Other", new ArrayBuffer[Column]) += col
    }
  })

  map.foreach(stat)


  // Number of tables, number of columns, raw data size
  def stat(entry: (String, Seq[Column])): Unit = {
    val numTables = cols.map(_.origin.toString).toSet.size
    val numColumns = cols.size
    val rawDataSize = cols.map(_.origin.toString).toSet.map(new File(_).length()).sum

    println("%s & %d, %d, %d".format(entry._1, numTables, numColumns, rawDataSize))
  }
}
