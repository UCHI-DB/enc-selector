package edu.uchicago.cs.encsel.app.encoding

import java.io.File
import java.util

import edu.uchicago.cs.encsel.classify.nn.NNPredictor
import edu.uchicago.cs.encsel.dataset.column.ColumnReaderFactory
import edu.uchicago.cs.encsel.dataset.feature.{Features, Filter}
import edu.uchicago.cs.encsel.parquet.{EncContext, ParquetWriterHelper}
import edu.uchicago.cs.encsel.dataset.schema.Schema
import edu.uchicago.cs.encsel.model.{DataType, IntEncoding, StringEncoding}
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.collection.JavaConversions._

object ParquetEncoder extends App {

  val inputFile = new File(args(0)).toURI
  val schemaFile = new File(args(1)).toURI
  val outputFile = new File(args(2)).toURI

  val intModel = args(3)
  val stringModel = args(4)
  val split = args(5)
  val sizeLimit = args.length match {
    case ge7 if ge7 >= 7 => args(6).toInt
    case _ => 1000000 // Default 1M
  }

  val schema = Schema.fromParquetFile(schemaFile)
  val parquetSchema = SchemaParser.toParquetSchema(schema)

  // Split file into columns
  val columnReader = ColumnReaderFactory.getColumnReader(inputFile)
  val columns = columnReader.readColumn(inputFile, schema)

  // See <code>edu.uchicago.cs.encsel.dataset.feature.Features
  val validFeatureIndex = Array(2, 3, 4, 5, 6, 8, 9, 10,
    11, 13, 15, 16, 18, 20,
    21, 23, 25, 26, 28)

  // Initialize predictor
  val intPredictor = new NNPredictor(intModel, validFeatureIndex.length)
  val stringPredictor = new NNPredictor(stringModel, validFeatureIndex.length)

  // For each column, extract features and run encoding selector
  val colEncodings = columns.map(col => {
    col.dataType match {
      case DataType.INTEGER => {
        val allFeatures = Features.extract(col, Filter.sizeFilter(sizeLimit), "temp_")
          .map(_.value).toArray
        val features = validFeatureIndex.map(allFeatures(_))
        val ret = intPredictor.predict(features)
        System.out.println("column name: "+ col.toString +" encoding: " + parquetIntEncoding(ret).toString)
        ret
      }
      case DataType.STRING => {
        val allFeatures = Features.extract(col, Filter.sizeFilter(sizeLimit), "temp_")
          .map(_.value).toArray
        val features = validFeatureIndex.map(allFeatures(_))
        val ret = stringPredictor.predict(features)
        System.out.println("column name: "+ col.toString +" encoding: " + parquetStringEncoding(ret).toString)
        ret
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
  var colNum = 0

  colEncodings.zip(parquetSchema.getColumns).foreach(pair => {
    val coldesc = pair._2

    coldesc.getType match {
      case PrimitiveTypeName.INT32 => {
        val encoding = args.length match {
          case ge8 if ge8 >= 8 => args(7).toInt // if user defined encoding
          case _ => pair._1
        }
        encodingMap.put(coldesc.toString, parquetIntEncoding(encoding))
        var intBitLength = 12
        var intBound = 2050
        System.out.println("column name: "+ coldesc.toString +" encoding: " + parquetIntEncoding(encoding).toString)
        if (encoding == 2){
          intBound = ParquetWriterHelper.scanIntMaxInTab(new File(inputFile).toURI, colNum, split,true)
          intBitLength = 32 - Integer.numberOfLeadingZeros(intBound)
          System.out.println("intBitLength: " + intBitLength + " intBound: " + intBound)
        }
        // TODO Determine bit size for integer, here hard code as a sample
        //System.out.println("intBitLength: " + intBitLength + " intBound: " + intBound)
        contextMap.put(coldesc.toString, Array[AnyRef](intBitLength.asInstanceOf[AnyRef], intBound.asInstanceOf[AnyRef]))
      }
      case PrimitiveTypeName.BINARY => {
        val encoding = args.length match {
          case ge9 if ge9 >= 9 => args(8).toInt // if user defined encoding
          case _ => pair._1
        }
        encodingMap.put(coldesc.toString, parquetStringEncoding(encoding))
        System.out.println("column name: "+ coldesc.toString +" encoding: " + parquetStringEncoding(encoding).toString)
      }
      case _ => {}
    }
    colNum = colNum+1
  })

  // Invoke Parquet Writer
  // TODO use CSV parser to parse file
  ParquetWriterHelper.write(inputFile, parquetSchema, outputFile, split, true)

  def parquetIntEncoding(enc: Int): Encoding = {
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

  def parquetStringEncoding(enc: Int): Encoding = {
    StringEncoding.values()(enc) match {
      case StringEncoding.PLAIN => Encoding.PLAIN
      case StringEncoding.DELTA => Encoding.DELTA_BYTE_ARRAY
      case StringEncoding.DELTAL => Encoding.DELTA_LENGTH_BYTE_ARRAY
      case StringEncoding.DICT => Encoding.PLAIN_DICTIONARY
      case _ => throw new IllegalArgumentException
    }
  }
}
