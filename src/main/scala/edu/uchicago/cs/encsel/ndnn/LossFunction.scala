/**
 * *****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 * *****************************************************************************
 */
package edu.uchicago.cs.encsel.ndnn

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.indexing.NDArrayIndex
import org.nd4j.linalg.ops.transforms.Transforms
import org.nd4j.linalg.factory.Nd4j

trait LossFunction {
  def forClassification: Boolean
  def loss(actual: Array[INDArray], expected: INDArray, fortest: Boolean = false): Double
  def gradient: Array[INDArray]
  def accuracy: Int
}

/**
 * Loss function dealing with a single output
 */
abstract class SimpleLoss extends LossFunction {
  protected var grad: INDArray = _
  protected var acc: Int = -1

  override def loss(actual: Array[INDArray], expected: INDArray, fortest: Boolean = false): Double = {
    loss(actual(0), expected, fortest)
  }

  def gradient = Array(grad)
  def accuracy = acc

  def loss(actual: INDArray, expected: INDArray, fortest: Boolean): Double
}

class SquareLoss extends SimpleLoss {

  def forClassification = false
  /**
   * @param actual		Shape [B, N]
   * @param	expected	Shape [B, N]
   * @return	The averaged squared difference of actual - expected
   */
  def loss(actual: INDArray, expected: INDArray, fortest: Boolean): Double = {
    val b = actual.shape()(0)

    if (!fortest) {
      // Compute Gradient
      grad = actual.sub(expected).mul(1d / b)
    }

    // Loss
    Transforms.pow(actual.sub(expected), 2).sum(1).meanNumber().doubleValue() / 2
  }

}

object SoftMaxLogLoss {
  val clip = 1e-12
}

class SoftMaxLogLoss extends SimpleLoss {

  def forClassification = true
  /**
   * @param	actual		Probability of each label. This is an <code>INDArray</code>
   * 									of shape [B,..., N], where B is the batch size, N is the dimension
   * @param	expected	The ground truth label. This is an <code>INDArray</code> of
   * 									shape [B,..., 1]
   * @return 	mean of log loss of ground truth label in actual probability
   *
   */
  def loss(actual: INDArray, expected: INDArray, fortest: Boolean): Double = {
    val shape = actual.shape()
    val b = shape(0)
    val n = shape.last
    val expectedSize = expected.shape.product

    val fetch = Index.get(actual, expected)
    val clipval = Transforms.max(fetch, SoftMaxLogLoss.clip, false)
    if (!fortest) {
      // Compute gradient
      grad = Nd4j.zerosLike(actual)
      // -log(x) gradient
      val allone = Nd4j.createUninitialized(expected.shape()).assign(1d / expectedSize)

      Index.put(grad, expected, allone.divi(clipval).negi())
    }
    if (forClassification) {
      // Accuracy for classification
      val predict = Nd4j.argMax(actual, actual.shape.length - 1)
      val eq = predict.eqi(expected)
      acc = eq.sumNumber().intValue()
    }
    Transforms.log(clipval, false).negi().meanNumber().doubleValue()
  }

}