/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query.offheap

import java.nio.ByteBuffer

class EqualScalar(val target: Int, val entryWidth: Int) extends Predicate {
  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val result = ByteBuffer.allocateDirect(Math.ceil(size.toDouble / 8).toInt)

    var counter = 0
    var buffer: Byte = 0
    for (i <- 0 until size) {
      val intIndex = i * entryWidth / 32
      val intOffset = i * entryWidth % 32

      val intValue = input.getLong(offset + intIndex * 4) >> intOffset
      val mask = ((1L << entryWidth) - 1)

      intValue & mask match {
        case target => buffer = (buffer | (1 << counter)).toByte
        case _ => {}
      }

      counter += 1
      if (counter == 8) {
        counter = 0
        result.put(buffer)
        buffer = 0
      }
    }
    if (counter != 0)
      result.put(buffer)
    result.flip
    return result
  }
}
