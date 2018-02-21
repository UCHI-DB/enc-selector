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

package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper;
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence;
import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.LongEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;

import javax.persistence.EntityManager;
import java.io.File;
import java.util.List;


public class PreEncodingTool {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("PreEncodingTool path index");
            return;
        }
        String path = args[0];
        int index = Integer.parseInt(args[1]);
        int intbound = ParquetWriterHelper.scanIntMaxInTab(new File(path).toURI(), index);
        int bitLength = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("intBitLength: "+ bitLength +" intBound: "+intbound);
    }
}
