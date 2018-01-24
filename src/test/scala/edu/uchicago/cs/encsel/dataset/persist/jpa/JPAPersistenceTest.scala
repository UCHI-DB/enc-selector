package edu.uchicago.cs.encsel.dataset.persist.jpa

import java.io.File
import java.util.ArrayList
import javax.persistence.PersistenceException

import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.Feature
import edu.uchicago.cs.encsel.model.DataType
import org.eclipse.persistence.exceptions.DatabaseException
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class JPAPersistenceTest {

  @Before
  def cleanSchema: Unit = {
    val em = JPAPersistence.emf.createEntityManager()
    em.getTransaction.begin()

    em.createNativeQuery("DELETE FROM feature WHERE 1 = 1;").executeUpdate()
    em.createNativeQuery("DELETE FROM col_data WHERE 1 = 1;").executeUpdate()
    em.flush()
    em.getTransaction.commit()

    em.getTransaction.begin()
    val col1 = new ColumnWrapper
    col1.id = 20
    col1.colName = "a"
    col1.colIndex = 5
    col1.dataType = DataType.STRING
    col1.colFile = new File("aab").toURI
    col1.origin = new File("ccd").toURI

    col1.features = new java.util.HashSet[Feature]

    var fea1 = new Feature
    fea1.name = "M"
    fea1.featureType = "P"
    fea1.value = 2.4

    col1.features += fea1

    em.persist(col1)

    em.getTransaction.commit()
    em.close()
  }

  @Test
  def testSaveNew: Unit = {
    val jpa = new JPAPersistence

    val col0 = new Column(new File("xmp").toURI, 9, "tpq", DataType.INTEGER)
    col0.colFile = new File("wpt").toURI
    col0.parent = jpa.find(20)

    val col1 = new Column(new File("dd").toURI, 3, "m", DataType.INTEGER)
    col1.colFile = new File("tt").toURI
    col1.parent = col0

    col1.features = new java.util.HashSet[Feature]

    val fea1 = new Feature("W", "A", 3.5)

    col1.features ++= Array(fea1)

    jpa.save(Array(col1))

    val cols = jpa.load()

    assertEquals(3, cols.size)

    cols.foreach(col => {
      col.colIndex match {
        case 3 => {
          assertEquals(DataType.INTEGER, col.dataType)
          assertEquals("m", col.colName)
          assertEquals(9, col.parent.colIndex)
          val feature = col.features.iterator.next
          assertEquals("W", feature.featureType)
          assertEquals("A", feature.name)
          assertEquals(3.5, feature.value, 0.01)
        }
        case 5 => {
          assertEquals(DataType.STRING, col.dataType)
          assertEquals("a", col.colName)
          val feature = col.features.iterator.next
          assertEquals("P", feature.featureType)
          assertEquals("M", feature.name)
          assertEquals(2.4, feature.value, 0.01)
        }
        case 9 => {
          assertEquals(DataType.INTEGER, col.dataType)
          assertEquals("tpq", col.colName)
          assertEquals(20, col.parent.colIndex)
        }
      }
    })
  }

  @Test
  def testSaveMerge: Unit = {
    val jpa = new JPAPersistence
    val cols = jpa.load().toArray
    assertEquals(1, cols.length)

    cols(0).features += new Feature("T", "PP", 3.25)
    jpa.save(cols)

    assertEquals(1, cols.length)
    val features = cols(0).features.toList
    assertEquals(2, features.size)
    assertEquals("PP", features(0).name)
    assertEquals("M", features(1).name)
  }

  @Test
  def testUpdate: Unit = {
    val jpa = new JPAPersistence

    val col1 = new ColumnWrapper()
    col1.origin = new File("dd").toURI
    col1.colIndex = 3
    col1.colName = "m"
    col1.dataType = DataType.INTEGER
    col1.id = 20
    col1.colFile = new File("tt").toURI

    col1.features = new java.util.HashSet[Feature]

    val fea1 = new Feature("W", "A", 3.5)

    col1.features ++= Array(fea1)

    jpa.save(Array[Column](col1))

    val cols = jpa.load().toArray

    assertEquals(1, cols.length)
    val col = cols(0)
    assertEquals(1, col.features.size())
  }

  @Test
  def testLoad: Unit = {
    val jpa = new JPAPersistence
    val cols = jpa.load().toArray

    assertEquals(1, cols.length)
    val col = cols(0)
    assertEquals(DataType.STRING, col.dataType)
    assertEquals(5, col.colIndex)
    assertEquals("a", col.colName)
  }

  @Test
  def testClean: Unit = {

  }
}