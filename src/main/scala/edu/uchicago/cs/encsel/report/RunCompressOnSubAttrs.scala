package edu.uchicago.cs.encsel.report

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.report.ParquetCompressFileSize
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.model.DataType

import scala.collection.JavaConverters._

object RunCompressOnSubAttrs extends App {

  val sql = "SELECT c FROM Column c WHERE c.parentWrapper IS NULL AND c.dataType = :dt ORDER BY c.id ASC"
  val childSql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent"

  val persist = new JPAPersistence
  val em = persist.em

  var counter = 0
  em.createQuery(sql, classOf[ColumnWrapper]).setParameter("dt", DataType.STRING).getResultList.asScala.foreach(col => {
    val ben = col.getInfo("subattr_benefit")
    if (ben > 0 && ben < 0.95) {
      counter+=1
      println("%d:%d".format(counter,col.id))
      val childrenCols = getChildren(col)
      childrenCols.foreach(child=>{
        val features = ParquetCompressFileSize.extract(col)
        col.replaceFeatures(features)
        persist.save(Seq(col))
      })
    }
  })

  def getChildren(col: Column): Seq[Column] = {
    em.createQuery(childSql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }
}
