package edu.uchicago.cs.encsel.classify

import edu.uchicago.cs.encsel.classify.nn.NNPredictor
import junit.framework.Assert
import org.junit.Test

class NNPredictorTest {

  @Test
  def testPredict:Unit = {
    val predictor = new NNPredictor("src/main/nnmodel/int_model",19)
    Assert.assertEquals(2,predictor.predict(Array.fill(19)(0f)))
  }
}
