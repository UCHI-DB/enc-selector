package edu.uchicago.cs.encsel.app.encoding

import java.io.File

import edu.uchicago.cs.encsel.classify.EncSelNNGraph
import edu.uchicago.cs.encsel.dataset.column.ColumnReaderFactory
import edu.uchicago.cs.encsel.dataset.feature.{Features, Filter}
import edu.uchicago.cs.encsel.dataset.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.schema.Schema
import edu.uchicago.cs.encsel.model.DataType
import edu.uchicago.cs.ndnn.FileStore

object ParquetEncoder extends App {

  val inputFile = new File(args(0)).toURI
  val schemaFile = new File(args(1)).toURI
  val outputFile = new File(args(2)).toURI

  val intModelFile = args(3)
  val stringModelFile = args(4)

  val schema = Schema.fromParquetFile(schemaFile)
  val parquetSchema = SchemaParser.toParquetSchema(schema)

  // Split file into columns
  val columnReader = ColumnReaderFactory.getColumnReader(inputFile)
  val columns = columnReader.readColumn(inputFile, schema)

  // Initialize classifier
  val intGraph = new EncSelNNGraph(10, 5)
  intGraph.load(new FileStore(intModelFile).load)
  val stringGraph = new EncSelNNGraph(10, 4)
  stringGraph.load(new FileStore(stringModelFile).load)

  // For each column, extract features and run encoding selector
  val colWithFeatures = columns.map(col => {
    col.dataType match {
      case DataType.INTEGER => {
        val features = Features.extract(col, Filter.sizeFilter(1000000), "temp_")
      }
      case DataType.STRING => {
        val features = Features.extract(col, Filter.sizeFilter(1000000), "temp_")
      }
      case _ => {
        null
      }
    }
  })

  // Setup encoding parameters
  // TODO Determine bitsize for integer

  // Invoke Parquet Writer
  // TODO user CSV parser to parse file
  ParquetWriterHelper.write(inputFile, parquetSchema, outputFile, ",")
}
