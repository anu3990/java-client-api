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

import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.spark.Reader.MarkLogicReadDataSource;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class MarkLogicSparkReadDriver {

    private static String collectionName = "bulkOutputTest";

    public static void main(String args[]) {
        IOTestUtil.writeDocuments(200,collectionName);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .getOrCreate();
        try {
            readInput(sparkSession);
        } catch(Exception e) {
            e.printStackTrace();
        }

    }
    private static void readInput(SparkSession sparkSession) {
        System.out.println("before readInput");

        DataFrameReader reader = sparkSession.read()
                .format(MarkLogicReadDataSource.class.getName())
                .option("host", "localhost")
                .option("port", port-number)
                .option("user", "username")
                .option("password","password")
                .option("batchsize", 10);

        StructType struct = new StructType()
                .add("docNum", DataTypes.IntegerType)
                .add("docName", DataTypes.StringType);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(struct);

        Dataset<Row> dataSet = reader.load();

        Long count = dataSet.map(row -> {
            System.out.println("Data is "+row.mkString(", "));
            return row;
        }
        , encoder).count();
        System.out.println("*********** Total Number of records pulled = "+ count);
        cleanup();
    }

    public static void cleanup() {

        QueryManager queryMgr = IOTestUtil.db.newQueryManager();
        DeleteQueryDefinition deletedef = queryMgr.newDeleteDefinition();
        deletedef.setCollections(collectionName);
        queryMgr.delete(deletedef);

    }
}
