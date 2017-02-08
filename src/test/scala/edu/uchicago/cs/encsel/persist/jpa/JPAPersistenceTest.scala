package edu.uchicago.cs.encsel.persist.jpa

import java.io.File
import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer

import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

import edu.uchicago.cs.encsel.column.Column
import edu.uchicago.cs.encsel.feature.Feature
import edu.uchicago.cs.encsel.model.DataType
import javax.persistence.Embeddable
import javax.persistence.Entity
import javax.persistence.Table

class JPAPersistenceTest {

  @Before
  def cleanSchema: Unit = {
    JPAPersistence.em.getTransaction.begin

    JPAPersistence.em.createNativeQuery("DELETE FROM feature WHERE 1 = 1;").executeUpdate()
    JPAPersistence.em.createNativeQuery("DELETE FROM col_data WHERE 1 = 1;").executeUpdate()
    JPAPersistence.em.flush()

    JPAPersistence.em.getTransaction.commit

    JPAPersistence.em.getTransaction.begin
    var col1 = new ColumnWrapper
    col1.colName = "a"
    col1.colIndex = 5
    col1.dataType = DataType.STRING
    col1.colFile = new File("aab").toURI
    col1.origin = new File("ccd").toURI

    col1.features = new ArrayList[FeatureWrapper]

    var fea1 = new FeatureWrapper
    fea1.name = "M"
    fea1.featureType = "P"
    fea1.value = 2.4

    col1.features += fea1

    JPAPersistence.em.persist(col1)

    JPAPersistence.em.getTransaction.commit
  }

  @Test
  def testSave: Unit = {
    var jpa = new JPAPersistence

    var col1 = new Column(new File("dd").toURI, 3, "m", DataType.INTEGER)
    col1.colFile = new File("tt").toURI

    col1.features = new ArrayList[Feature]

    var fea1 = new Feature("W", "A", 3.5)

    col1.features = Array(fea1)

    jpa.save(Array(col1))

    var cols = jpa.load()

    assertEquals(2, cols.size)

    cols.foreach(col => {
      col.colIndex match {
        case 3 => {
          assertEquals(DataType.INTEGER, col.dataType)
          assertEquals("m", col.colName)
          var feature = col.features.iterator.next
          assertEquals("W", feature.featureType)
          assertEquals("A", feature.name)
          assertEquals(3.5, feature.value, 0.01)
        }
        case 5 => {
          assertEquals(DataType.STRING, col.dataType)
          assertEquals("a", col.colName)
          var feature = col.features.iterator.next
          assertEquals("P", feature.featureType)
          assertEquals("M", feature.name)
          assertEquals(2.4, feature.value, 0.01)
        }
      }
    })
  }

  @Test
  def testLoad: Unit = {
    var jpa = new JPAPersistence
    var cols = jpa.load()

    assertEquals(1, cols.size)
    var col = cols.iterator.next()
    assertEquals(DataType.STRING, col.dataType)
    assertEquals(5, col.colIndex)
    assertEquals("a", col.colName)
  }

  @Test
  def testClean: Unit = {

  }
}