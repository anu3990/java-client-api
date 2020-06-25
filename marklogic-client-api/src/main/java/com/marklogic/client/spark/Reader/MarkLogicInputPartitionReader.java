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
package com.marklogic.client.spark.Reader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.dataservices.OutputEndpoint;
import com.marklogic.client.io.JacksonHandle;

import com.marklogic.client.spark.Common;
import com.marklogic.client.spark.IOTestUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.RowFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MarkLogicInputPartitionReader implements InputPartitionReader<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(MarkLogicInputPartitionReader.class);
    ObjectNode apiObj;
    static String apiName;
    static String scriptPath;
    static String apiPath;
    OutputEndpoint.BulkOutputCaller bulkCaller;
    DatabaseClient db;
    private String forestName;
    private InputStream[] output = null;
    List<InternalRow> rows;
    ObjectNode value;
    int index;

    public MarkLogicInputPartitionReader(String forestName, Map<String, String> map) {
        this.forestName = forestName;
        this.rows = new ArrayList<>();
        this.index = 0;
        apiName = "bulkOutputCallerNext.api";


        try {
            db = DatabaseClientFactory.newClient(map.get("host"), Integer.valueOf(map.get("port")),
                    new DatabaseClientFactory.DigestAuthContext(map.get("user"), map.get("password")),
                    Common.CONNECTION_TYPE);
            apiObj = IOTestUtil.readApi(apiName);
            scriptPath = IOTestUtil.getScriptPath(apiObj);
            apiPath = IOTestUtil.getApiPath(scriptPath);
            IOTestUtil.load(apiName, apiObj, scriptPath, apiPath);
            logger.info("******************* forestName ****************** " + forestName);
            String endpointState = "{\"next\":" + 1 + "}";
            String workUnit = "{\"limit\":" + Integer.valueOf(map.get("batchsize")) + ", \"forestName\":\"" + forestName + "\"}";
            bulkCaller = OutputEndpoint.on(db, new JacksonHandle(apiObj)).bulkCaller();
            bulkCaller.setEndpointState(new ByteArrayInputStream(endpointState.getBytes()));
            bulkCaller.setWorkUnit(new ByteArrayInputStream(workUnit.getBytes()));
            this.output = bulkCaller.next();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    @Override
    public boolean next() {
        try {

            if(this.index >= this.output.length) {
                this.output = bulkCaller.next();
                this.index = 0;
            }
            if(this.output == null || this.output.length ==0) {
                return false;
            }
            else {
                this.value = IOTestUtil.mapper.readValue(output[this.index], ObjectNode.class);

                this.index+=1;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return true;
    }

    @Override
    public InternalRow get(){

        try {

            Row row = RowFactory.create(this.value.get("docNum").asInt(), this.value.get("docName").asText());
            StructType struct = new StructType()
                    .add("docNum", DataTypes.IntegerType)
                    .add("docName", DataTypes.StringType);
            ExpressionEncoder<Row> encoder = RowEncoder.apply(struct);
            InternalRow internalRow = encoder.toRow(row);
            return internalRow;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);

        }
        return null;
    }

    @Override
    public void close() throws IOException {
        System.out.println("Stopping");
    }
}
