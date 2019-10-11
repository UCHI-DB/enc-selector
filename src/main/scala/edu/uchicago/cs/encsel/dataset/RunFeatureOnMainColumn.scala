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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.dataset

import edu.uchicago.cs.encsel.dataset.RunFeature.featureRunner
import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.encode.orc.OrcEncFileSize
import edu.uchicago.cs.encsel.dataset.persist.Persistence
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._

/**
  * Created by harper on 4/23/17.
  */
object RunFeatureOnMainColumn extends App {

  val featureRunner = new FeatureRunner {
    override def getColumns(persistence: Persistence) = {
      val data = persistence.asInstanceOf[JPAPersistence].ems.get
        .createNativeQuery("SELECT \n    cd.*\nFROM\n    col_data cd\nWHERE\n    cd.parent_id IS NULL AND NOT EXISTS (SELECT 1 FROM feature f where f.col_id = cd.id and f.name = 'ORC_file_size')",
          classOf[ColumnWrapper]).getResultList
      data.asScala.map(_.asInstanceOf[Column]).toList
    }
  }

  featureRunner.missed = Set(OrcEncFileSize)
  featureRunner.run(args)
}
