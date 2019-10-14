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

package org.apache.parquet.column.page;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

import static org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;

/**
 * This page does not decompress data until the content is being accessed
 */
public class CompressedDataPageV1 extends DataPageV1 {

    BytesInput compressedData;

    BytesDecompressor decompressor;

    boolean compressed = true;

    public CompressedDataPageV1(DataPageV1 dataPage, BytesDecompressor decompressor) {
        super(BytesInput.empty(), dataPage.getValueCount(), dataPage.getUncompressedSize(), dataPage.getStatistics(),
                dataPage.getRlEncoding(), dataPage.getDlEncoding(), dataPage.getValueEncoding());
        this.compressedData = dataPage.getBytes();
        this.decompressor = decompressor;
    }

    @Override
    public BytesInput getBytes() {
        try {
            if (compressed) {
                compressedData = this.decompressor.decompress(compressedData, getUncompressedSize());
                compressed = false;
            }
            return compressedData;
        } catch (IOException e) {
            throw new ParquetEncodingException("cannot decompress page", e);
        }
    }
}
