package edu.uchicago.cs.encsel.ndnn

import org.junit.Assert.assertEquals
import org.junit.Test
import org.nd4j.linalg.factory.Nd4j

class UpdatePolicyTest {

  @Test
  def testSGDUpdate: Unit = {
    val env = new NodeEnv {}
    val param = new Param(env)
    param.setValue(Nd4j.createUninitialized(Array(4, 3, 2)).assign(5))
    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(2)

    val sgd = new SGD(0.2)
    sgd.update(param)

    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.6, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(3)
    sgd.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.0, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(7)
    sgd.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(2.6, param.value.getDouble(i, j, k), 0.001)
    }
  }

  @Test
  def testMomentumUpdate: Unit = {
    val env = new NodeEnv {}
    val param = new Param(env)
    param.setValue(Nd4j.createUninitialized(Array(4, 3, 2)).assign(5))
    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(2)

    val momentum = new Momentum(0.2, 0.9)

    momentum.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.6, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(3)
    momentum.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.18, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(7)
    momentum.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(3.662, param.value.getDouble(i, j, k), 0.001)
    }
  }

  @Test
  def testRmsPropUpdate: Unit = {

    val env = new NodeEnv {}
    val param = new Param(env)
    param.setValue(Nd4j.createUninitialized(Array(4, 3, 2)).assign(5))
    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(2)

    val rms = new RMSProp(0.2, 0.9)

    rms.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.8, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(3)
    rms.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.517, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(7)
    rms.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.049, param.value.getDouble(i, j, k), 0.001)
    }
  }

  @Test
  def testAdamUpdate: Unit = {

    val env = new NodeEnv {}
    val param = new Param(env)
    param.setValue(Nd4j.createUninitialized(Array(4, 3, 2)).assign(5))
    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(2)

    val adam = new Adam(0.2, 0.9, 0.8)

    adam.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.8, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(3)
    adam.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.612, param.value.getDouble(i, j, k), 0.001)
    }

    param.grad = Nd4j.createUninitialized(Array(4, 3, 2)).assign(7)
    adam.update(param)
    for (i <- 0 until 4; j <- 0 until 3; k <- 0 until 2) {
      assertEquals(4.473, param.value.getDouble(i, j, k), 0.001)
    }
  }
}