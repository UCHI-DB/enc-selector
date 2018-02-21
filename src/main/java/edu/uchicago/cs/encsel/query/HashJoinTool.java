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
 *     Chunwei Liu - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query;

import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.perf.Profiler;
import edu.uchicago.cs.encsel.query.operator.HashJoin;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import scala.Tuple2;

import java.io.File;


public class HashJoinTool {

    public static void main(String[] args) {
        int repeat = 5;
        long clocktime = 0L;
        long cputime = 0L;
        long usertime = 0L;
        Tuple2<Object,Object> joinindex = new Tuple2<Object,Object>(0, 1);
        for (int i = 0; i < repeat; i++) {
            Profiler profiler = new Profiler();
            profiler.mark();
            TempTable result = new HashJoin().join(new File("/home/cc/tpch-generator/dbgen/part.parquet").toURI(), TPCHSchema.partSchema(),
                    new File("/home/cc/tpch-generator/dbgen/lineitem.parquet").toURI(), TPCHSchema.lineitemSchema(),
                    joinindex, new int[] {0}, new int[] {5,6});
            profiler.pause();
            ColumnTempTable tab = (ColumnTempTable) result;

            System.out.println(tab.getColumns().length);
            Column[] cols = tab.getColumns();
            Column col1 = cols[0];
            System.out.println(col1.getData().size());
            clocktime = clocktime + profiler.wcsum();
            cputime = cputime + profiler.cpusum();
            usertime = usertime + profiler.usersum();
            System.out.println("user:" + profiler.usersum() + ", CPUsum:" + profiler.cpusum() + ", wcsum:" + profiler.wcsum());

        }
        System.out.println(String.format("%s,%d,%d,%d", "Hashjoin", clocktime / repeat, cputime / repeat, usertime / repeat));
    }
}


