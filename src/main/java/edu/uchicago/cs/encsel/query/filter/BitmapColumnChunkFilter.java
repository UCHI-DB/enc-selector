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

package edu.uchicago.cs.encsel.query.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;

import org.apache.parquet.hadoop.metadata.BlockMetaData;

import edu.uchicago.cs.encsel.query.bitmap.Bitmap;

public class BitmapColumnChunkFilter {

    public static List<BlockMetaData> BitmapPageFilter(Bitmap bm,  List<BlockMetaData> blocks, ColumnDescriptor path) {
    	  
    	  List<BlockMetaData> filteredBlocks = new ArrayList<BlockMetaData>();
    	  long rowCount = 0;
    	  boolean drop = true;
    	  
	  for (BlockMetaData block : blocks) {
		long startCount = rowCount;
		long endCount = startCount + block.getRowCount();
		for (long i = startCount; i < endCount; i++) {
		  if (bm.test(i)==true) {
			drop = false;
			break;
		  }
		}
		
	    if(!drop) {
	      filteredBlocks.add(block);
	    }
	    rowCount = endCount;
	  }
	  
	  return filteredBlocks;
    }

}
