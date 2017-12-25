package edu.uchicago.cs.encsel.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.dataset.parquet.{EncContext, ParquetWriterHelper}
import edu.uchicago.cs.encsel.query.VColumnPredicate
import edu.uchicago.cs.encsel.query.operator.VerticalSelect
import edu.uchicago.cs.encsel.query.tpch.ScanGenData.{intEncodings, stringEncodings}
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

object Performance {

  val TPCH_FOLDER = "/home/harper/TPCH"

  def genInt(schema: MessageType, outputFolder: String, index: Int): Unit = {
    val inputFile = new File(TPCH_FOLDER + "/lineitem.tbl").toURI

    // Encode lineitem.line_number with different encodings
    intEncodings.foreach(intEncoding => {
      EncContext.encoding.get().put(schema.getColumns()(index).toString, intEncoding)

      ParquetWriterHelper.write(inputFile, schema,
        new File("%s/%s".format(outputFolder, intEncoding.name())).toURI, "\\|", false)
    })
  }

  def genString(schema: MessageType, outputFolder: String, index: Int): Unit = {
    val inputFile = new File(TPCH_FOLDER + "/lineitem.tbl").toURI

    // Encode lineitem.part_key with different encodings
    // Encode lineitem.line_number with different encodings
    stringEncodings.foreach(stringEncoding => {
      EncContext.encoding.get().put(schema.getColumns()(index).toString, stringEncoding)

      ParquetWriterHelper.write(inputFile, schema,
        new File("%s/%s".format(outputFolder, stringEncoding.name())).toURI, "\\|", false)
    })
  }

  def scan(schema: MessageType, outputPath: String, index: Int, predicate: Any => Boolean): Unit = {
    val folder = new File(outputPath)
    folder.listFiles().foreach(f => {
      try {
        Encoding.valueOf(f.getName)
        val start = System.currentTimeMillis()
        new VerticalSelect().select(f.toURI,
          new VColumnPredicate(predicate, index),
          schema,
          Array(index)
        )
        println("%s:%d".format(f.getName, System.currentTimeMillis() - start))
      } catch {
        case e: IllegalArgumentException => {}
      }
    })
  }
}
