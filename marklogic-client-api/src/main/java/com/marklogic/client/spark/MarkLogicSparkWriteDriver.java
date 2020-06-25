/*
 * Copyright 2020 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.spark;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.spark.Writer.MarkLogicWriteDataSource;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;


public class MarkLogicSparkWriteDriver {

    // Adding below variables for easy cleanup.
    static int numberOfPartitions = 4;
    static Long recordCount;
    public static List<Long> taskIds;

    public static void main(String args[]) {
        taskIds = new ArrayList<>();

        SparkSession sparkSession = SparkSession.builder()
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("header", true)
                .format("csv")
                .csv("s3-path"); // Absolute path to data.csv
        StringBuffer headers = new StringBuffer();
        for(int i=0; i<dataset.schema().fields().length; i++) {
            headers.append(dataset.schema().fields()[i].name());
            headers.append(",");
        }
        System.out.println("************ Starting MarkLogicSparkWriteDriver ****************");
        try {
            DataFrameWriter writer = dataset.write().format(MarkLogicWriteDataSource.class.getName())
                    .option("host", "ip-address")
                    .option("port", 8012)
                    .option("user", "admin")
                    .option("password","admin")
                    .option("batchsize", 10)
                    .option("schema", headers.toString());
            writer.save();

        } catch(Exception e) {
            e.printStackTrace();
        }
        recordCount = dataset.count();
       // cleanup();
    }
    public static void cleanup() {
        JSONDocumentManager docMgr = IOTestUtil.db.newJSONDocumentManager();
        System.out.println("**************** Cleaning Up ********************");
        for(Long j:taskIds) {
            for (int i = 1; i <= (recordCount / numberOfPartitions)+1; i++) {
                String uri = "/marklogic/ds/test/bulkInputCaller/"+j+"/" + i + ".json";
                docMgr.delete(uri);
            }
        }

    }
}
