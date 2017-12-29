package edu.uchicago.cs.encsel.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.dataset.parquet.{EncContext, ParquetWriterHelper}
import org.apache.parquet.column.Encoding

import scala.collection.JavaConversions._

object LoadTPCH extends App {

  //  val folder = "/home/harper/TPCH/"
  val folder = args(0)
  val inputsuffix = ".tbl"
  val outputsuffix = ".parquet"

  // Load TPCH
  TPCHSchema.schemas.foreach(schema => {
    ParquetWriterHelper.write(
      new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
      schema,
      new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI, "\\|", false)
  })

}

object LoadTPCHTest extends App {
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(1).toString, Encoding.BIT_PACKED)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(1).toString, Array("18", "200000"))
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(3).toString, Encoding.BIT_PACKED)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(3).toString, Array("3", "7"))
  ParquetWriterHelper.write(new File("/home/harper/TPCH/lineitem_100").toURI, TPCHSchema.lineitemSchema,
    new File("/home/harper/TPCH/lineitem_100.parquet").toURI, "\\|", false)
}