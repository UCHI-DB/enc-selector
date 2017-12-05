package edu.uchicago.cs.encsel.app.encoding

import java.io.File

import edu.uchicago.cs.encsel.dataset.column.ColumnReaderFactory
import edu.uchicago.cs.encsel.dataset.feature.{Features, Filter}
import edu.uchicago.cs.encsel.dataset.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.schema.Schema

object ParquetEncoder extends App {

  val inputFile = new File(args(0)).toURI
  val schemaFile = new File(args(1)).toURI
  val outputFile = new File(args(2)).toURI

  val intModelFile = new File(args(3))
  val stringModelFile = new File(args(4))

  val schema = Schema.fromParquetFile(schemaFile)
  val parquetSchema = SchemaParser.toParquetSchema(schema)

  // Split file into columns
  val columnReader = ColumnReaderFactory.getColumnReader(inputFile)
  val columns = columnReader.readColumn(inputFile, schema)

  // Initialize classifier

  // For each column, extract features and run encoding selector
  val colWithFeatures = columns.map(col => {
    val features = Features.extract(col, Filter.sizeFilter(1000000), "temp_")

  })

  // Setup encoding parameters
  // TODO Determine bitsize for integer

  // Invoke Parquet Writer
  // TODO user CSV parser to parse file
  ParquetWriterHelper.write(inputFile,parquetSchema,outputFile,",")
}
