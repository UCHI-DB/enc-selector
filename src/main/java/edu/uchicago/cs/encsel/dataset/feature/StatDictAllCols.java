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

package edu.uchicago.cs.encsel.dataset.feature;

import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper;
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence;

import javax.persistence.EntityManager;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.List;


public class StatDictAllCols {

    public static void main(String[] args) throws Exception {
        final int start = (args.length >= 1) ? Integer.parseInt(args[0]) : 0;
        int batch = 2000;
        System.out.println(String.format("Start is %d", start));
        String ofd = "src/test/resource/encoding/dictStat.csv";
        File lOutput = new File(ofd);
        System.out.println(lOutput.toURI().toString());
        if (lOutput.exists())
            lOutput.delete();
        FileWriter fileWriter = new FileWriter(ofd);
        fileWriter.append("COL_ID,TYPE,ORIGIN,L_DICT_ENC,L_BP_ENC,L_DCIT,G_DICT_ENC,G_BP_ENC,G_DCIT\n");
        EntityManager em = JPAPersistence.emf().createEntityManager();
        List<ColumnWrapper> columns = em.createQuery("select c from Column c where c.parentWrapper IS NULL", ColumnWrapper.class).getResultList();
        System.out.println(String.format("Total number of cols is %d", columns.size()));
        URI curUri;
        String stat;
        for (ColumnWrapper col : columns){
            System.out.println(col.id());
            if (col.id() >= start) {
                curUri = col.colFile();
                URI lUri = DictionaryEncoder.genOutputURI(curUri, "LDICTENCODING");
                URI lBPUri = DictionaryEncoder.genOutputURI(curUri, "LBPDICTENCODING");
                URI lDictUri = DictionaryEncoder.genOutputURI(curUri, "LOCALDICT");
                URI gUri = DictionaryEncoder.genOutputURI(curUri, "GDICTENCODING");
                URI gBPUri = DictionaryEncoder.genOutputURI(curUri, "GBPDICTENCODING");
                URI gDictUri = DictionaryEncoder.genOutputURI(curUri, "GLOBALDICT");
                stat = col.id()+","+col.dataType() + "," +new File(curUri).length()+","+new File(lUri).length()+","+
                        new File(lBPUri).length()+","+new File(lDictUri).length()+","+new File(gUri).length()+","+new File(gBPUri).length()+","+
                        new File(gDictUri).length();
                fileWriter.append(stat+"\n");
                //System.out.println(stat);
            }
        }
        fileWriter.flush();
        fileWriter.close();
    }
}
