package edu.uchicago.cs.encsel.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.dataset.parquet.ParquetReaderHelper.ReaderProcessor
import edu.uchicago.cs.encsel.dataset.parquet.{EncContext, ParquetReaderHelper, ParquetWriterHelper}
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter
import org.apache.parquet.VersionParser
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConversions._

object Sketch extends App {

  val schema = new MessageType("default",
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "v1"),

    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "v2")
  )

  write
  read

  def write: Unit = {
    EncContext.encoding.get().put(schema.getColumns()(0).toString, Encoding.PLAIN)
    EncContext.context.get().put(schema.getColumns()(0).toString, Array[AnyRef]("0", "0"));
    EncContext.encoding.get().put(schema.getColumns()(1).toString, Encoding.PLAIN)
    EncContext.context.get().put(schema.getColumns()(1).toString, Array[AnyRef]("0", "0"));
    ParquetWriterHelper.write(new File("/home/harper/temp/test.tmp").toURI, schema,
      new File("/home/harper/temp/test.tmp.PLAIN").toURI, ",", false)
  }

  def read: Unit = {
    ParquetReaderHelper.read(new File("/home/harper/temp/test.tmp.PLAIN").toURI,
      new ReaderProcessor() {
        override def processFooter(footer: Footer): Unit = {}

        override def processRowGroup(version: VersionParser.ParsedVersion,
                                     meta: BlockMetaData, rowGroup: PageReadStore): Unit = {
          val col = schema.getColumns()(1)
          val colreader = new ColumnReaderImpl(col, rowGroup.getPageReader(col), new NonePrimitiveConverter, version)
          for (i <- 0L until colreader.getTotalValueCount) {
            if (colreader.getCurrentDefinitionLevel == 1)
              colreader.getInteger()
            colreader.consume()
          }
        }
      })
  }
}
