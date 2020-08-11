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

import java.util.*;

public class MarkLogicSparkWriteDriver {

    // Adding below variables for easy cleanup.
    static int numberOfPartitions = 4;
    static Long recordCount;
    public List<Long> taskIds;

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .getOrCreate();
        MarkLogicSparkWriteDriver ml = new MarkLogicSparkWriteDriver();
        Dataset dataset1 = sparkSession.read()
                .format("json")
                .json("/Users/asinha/intellij/july-8/java-client-api/marklogic-client-api/src/main/java/com/marklogic/client/spark/data/test.json");
                //.csv("/Users/asinha/intellij/july-8/java-client-api/marklogic-client-api/src/main/java/com/marklogic/client/spark/data/data.csv");
        ml.mlSparkDriver(dataset1, "hostname", 8010, "username", "password", "/demo", "data-hub-MODULES");
    }

    public void mlSparkDriver(Dataset dataset, String host, int port, String username, String password, String prefix, String moduleDatabase) {

        System.out.println("************ Starting MarkLogicSparkWriteDriver **************** "+dataset.schema());
        try {
            DataFrameWriter writer = dataset.write().format(MarkLogicWriteDataSource.class.getName())
                    .option("host", host)
                    .option("port", port)
                    .option("user", username)
                    .option("password",password)
                    .option("prefixvalue", prefix)
                    .option("moduledatabase", moduleDatabase)
                    .option("batchsize", 10);

            writer.save();

        } catch(Exception e) {
            e.printStackTrace();
        }
        //recordCount = dataset.count();
        // cleanup();
    }
    public void cleanup() {
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
