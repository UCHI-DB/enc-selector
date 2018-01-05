package edu.uchicago.cs.encsel.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper.ReaderProcessor
import edu.uchicago.cs.encsel.dataset.parquet.{ParquetReaderHelper, ParquetWriterHelper}
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter
import org.apache.parquet.VersionParser
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConversions._

object LoadTPCH extends App {

  val folder = "/home/harper/TPCH/"
  //  val folder = args(0)
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
  ParquetWriterHelper.write(new File("/home/harper/TPCH/lineitem.tbl").toURI, TPCHSchema.lineitemOptSchema,
    new File("/home/harper/TPCH/opt/lineitem.parquet").toURI, "\\|", false)
}

object SelectLineitem extends App {
  val schema = TPCHSchema.lineitemOptSchema
  val start = System.currentTimeMillis()
  ParquetReaderHelper.read(new File("/home/harper/TPCH/opt/lineitem.parquet").toURI, new ReaderProcessor() {
    override def processFooter(footer: Footer): Unit = {

    }

    override def processRowGroup(version: VersionParser.ParsedVersion,
                                 meta: BlockMetaData, rowGroup: PageReadStore): Unit = {

      val readers = schema.getColumns.map(col => {
        new ColumnReaderImpl(col, rowGroup.getPageReader(col), new NonePrimitiveConverter, version)
      })

      readers.foreach(reader => {
        for (i <- 0L until rowGroup.getRowCount) {
          if (reader.getCurrentDefinitionLevel > 0)
            reader.readValue()
          reader.consume()
        }
      })
    }
  })
  println(System.currentTimeMillis() - start)
}