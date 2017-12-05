package edu.uchicago.cs.encsel.app.encoding

import java.io.File
import java.util

import edu.uchicago.cs.encsel.classify.EncSelNNGraph
import edu.uchicago.cs.encsel.dataset.column.ColumnReaderFactory
import edu.uchicago.cs.encsel.dataset.feature.{Features, Filter}
import edu.uchicago.cs.encsel.dataset.parquet.{EncContext, ParquetWriterHelper}
import edu.uchicago.cs.encsel.dataset.schema.Schema
import edu.uchicago.cs.encsel.model.{DataType, IntEncoding, StringEncoding}
import edu.uchicago.cs.ndnn.FileStore
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.collection.JavaConversions._

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
  val intGraph = new EncSelNNGraph(17, 5)
  intGraph.load(new FileStore(intModelFile).load)
  val stringGraph = new EncSelNNGraph(11, 4)
  stringGraph.load(new FileStore(stringModelFile).load)

  // For each column, extract features and run encoding selector
  val colEncodings = columns.map(col => {
    col.dataType match {
      case DataType.INTEGER => {
        val features = Features.extract(col, Filter.sizeFilter(1000000), "temp_")
        intGraph.getInputs.zip(features).foreach(pair => {
          val input = pair._1
          val feature = pair._2
          input.set(feature.value)
        })
        intGraph.forward()
        intGraph.getOutput.getValue.getInt(0)
      }
      case DataType.STRING => {
        val features = Features.extract(col, Filter.sizeFilter(1000000), "temp_")
        stringGraph.getInputs.zip(features).foreach(pair => {
          val input = pair._1
          val feature = pair._2
          input.set(feature.value)
        })
        stringGraph.forward()
        stringGraph.getOutput.getValue.getInt(0)
      }
      case _ => {
        -1
      }
    }
  })
  // Setup encoding parameters
  val encodingMap = new util.HashMap[String, Encoding]()
  EncContext.encoding.set(encodingMap)
  val contextMap = new util.HashMap[String, Array[AnyRef]]()
  EncContext.context.set(contextMap)

  colEncodings.zip(parquetSchema.getColumns).foreach(pair => {
    val encoding = pair._1
    val coldesc = pair._2

    coldesc.getType match {
      case PrimitiveTypeName.INT32 => {
        encodingMap.put(coldesc.toString, parquetIntEncoding(encoding))
        // TODO Determine bit size for integer, here hardcode as a sample
        contextMap.put(coldesc.toString, Array[AnyRef]("16", "1024"))
      }
      case PrimitiveTypeName.BINARY => {
        encodingMap.put(coldesc.toString, parquetStringEncoding(encoding))
      }
      case _ => {}
    }
  })

  // Invoke Parquet Writer
  // TODO user CSV parser to parse file
  ParquetWriterHelper.write(inputFile, parquetSchema, outputFile, ",")

  def parquetStringEncoding(enc: Int): Encoding = {
    IntEncoding.values()(enc) match {
      case IntEncoding.PLAIN => {
        Encoding.PLAIN
      }
      case IntEncoding.BP => {
        Encoding.BIT_PACKED
      }
      case IntEncoding.RLE => {
        Encoding.RLE
      }
      case IntEncoding.DELTABP => {
        Encoding.DELTA_BINARY_PACKED
      }
      case IntEncoding.DICT => {
        Encoding.PLAIN_DICTIONARY
      }
      case _ => {
        throw new IllegalArgumentException
      }
    }
  }

  def parquetIntEncoding(enc: Int): Encoding = {
    StringEncoding.values()(enc) match {
      case StringEncoding.PLAIN => Encoding.PLAIN
      case StringEncoding.DELTA => Encoding.DELTA_BYTE_ARRAY
      case StringEncoding.DELTAL => Encoding.DELTA_LENGTH_BYTE_ARRAY
      case StringEncoding.DICT => Encoding.PLAIN_DICTIONARY
      case _ => throw new IllegalArgumentException
    }
  }
}
