package edu.uchicago.cs.encsel.query

import edu.uchicago.cs.encsel.query.BitwiseOpr.BitwiseOpr
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

trait Predicate {
  def leaves: Iterable[ColumnPredicate]
}

trait PredicatePipe {
  def pipe(d: Double) = {};

  def pipe(b: Binary) = {};

  def pipe(i: Int) = {};

  def pipe(l: Long) = {};

  def pipe(bl: Boolean) = {};

  def pipe(f: Float) = {};
}

abstract class ColumnPredicate(val predicate: (Any) => Boolean, val colIndex: Int) extends Predicate {

  var column: ColumnReader = null

  var pipe: PredicatePipe = new PredicatePipe() {};

  var processor: () => Boolean = () => {
    false
  };

  def setColumn(column: ColumnReader) = {
    this.column = column
    processor = column.getDescriptor.getType match {
      case PrimitiveTypeName.DOUBLE => {
        () => {
          predicate(column.getDouble) match {
            case true => {
              pipe.pipe(column.getDouble)
              true
            }
            case _ => false
          }
        }
      }
      case PrimitiveTypeName.BINARY => {
        () => {
          predicate(column.getBinary) match {
            case true => {
              pipe.pipe(column.getBinary)
              true
            }
            case _ => false
          }
        }
      }
      case PrimitiveTypeName.INT32 => {
        () => {
          predicate(column.getInteger) match {
            case true => {
              pipe.pipe(column.getInteger)
              true
            }
            case _ => false
          }
        }
      }
      case PrimitiveTypeName.INT64 => {
        () => {
          predicate(column.getLong) match {
            case true => {
              pipe.pipe(column.getLong)
              true
            }
            case _ => false
          }
        }
      }
      case PrimitiveTypeName.FLOAT => {
        () => {
          predicate(column.getFloat) match {
            case true => {
              pipe.pipe(column.getFloat)
              true
            }
            case _ => false
          }
        }
      }
      case PrimitiveTypeName.BOOLEAN => {
        () => {
          predicate(column.getBoolean) match {
            case true => {
              pipe.pipe(column.getBoolean)
              true
            }
            case _ => false
          }
        }
      }
      case _ => throw new IllegalArgumentException
    }
  }

  def setPipe(p: PredicatePipe) = this.pipe = p;

  def test(): Boolean = {
    val result = processor()
    column.consume()
    result
  }

  def leaves = Iterable[ColumnPredicate](this)
}

abstract class GroupPredicate(children: Iterable[Predicate]) extends Predicate {
  override def leaves: Iterable[ColumnPredicate] = {
    children.flatMap(_.leaves)
  }
}

trait HPredicate extends Predicate {
  def value: Boolean
}

class HColumnPredicate(p: Any => Boolean, colIndex: Int)
  extends ColumnPredicate(p, colIndex)
    with HPredicate {
  override def value: Boolean = test
}

trait VPredicate extends Predicate {
  def bitmap: Bitmap
}

class VColumnPredicate(predicate: Any => Boolean, colIndex: Int)
  extends ColumnPredicate(predicate, colIndex)
    with VPredicate {
  def bitmap: Bitmap = {
    val bitmap = new Bitmap(column.getTotalValueCount)
    for (i <- 0L until column.getTotalValueCount) {
      bitmap.set(i, test())
    }
    bitmap
  }
}

class VBitwisePredicate(val opr: BitwiseOpr, val left: VPredicate, val right: VPredicate)
  extends GroupPredicate(Iterable(left, right)) with VPredicate {

  def bitmap: Bitmap = {
    val leftbm = left.bitmap
    val rightbm = right match {
      case null => null
      case _ => right.bitmap
    }
    leftbm.compute(opr, rightbm)
  }


}
