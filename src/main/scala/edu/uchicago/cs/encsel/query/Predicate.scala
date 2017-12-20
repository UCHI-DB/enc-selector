package edu.uchicago.cs.encsel.query

import edu.uchicago.cs.encsel.query.BitwiseOpr.BitwiseOpr
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

trait Predicate {
  def leaves: Iterable[ColumnPredicate]
}

abstract class ColumnPredicate(val predicate: (Any) => Boolean, val colIndex: Int) extends Predicate {

  var column: ColumnReader = null

  def setColumn(column: ColumnReader) = this.column = column

  def test(): Boolean = {
    column.getDescriptor.getType match {
      case PrimitiveTypeName.DOUBLE => predicate(column.getDouble)
      case PrimitiveTypeName.BINARY => predicate(column.getBinary)
      case PrimitiveTypeName.INT32 => predicate(column.getInteger)
      case PrimitiveTypeName.INT64 => predicate(column.getLong)
      case PrimitiveTypeName.FLOAT => predicate(column.getFloat)
      case PrimitiveTypeName.BOOLEAN => predicate(column.getBoolean)
      case _ => throw new IllegalArgumentException
    }
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

class HColumnPredicate(p: Any => Boolean, colIndex: Int) extends ColumnPredicate(p, colIndex)
  with HPredicate {
  override def value: Boolean = test
}

trait VPredicate extends Predicate {
  def bitmap: Bitmap
}

class VColumnPredicate(predicate: Any => Boolean, colIndex: Int) extends ColumnPredicate(predicate, colIndex)
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
