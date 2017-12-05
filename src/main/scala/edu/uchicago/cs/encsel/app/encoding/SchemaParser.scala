package edu.uchicago.cs.encsel.app.encoding

import java.net.URI

import edu.uchicago.cs.encsel.dataset.schema.Schema
import org.apache.parquet.schema.MessageType

object SchemaParser {

  def toParquetSchema(schema: Schema): MessageType = {
    throw new UnsupportedOperationException
  }
}
