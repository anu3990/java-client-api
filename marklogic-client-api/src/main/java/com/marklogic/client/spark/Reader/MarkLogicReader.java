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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MarkLogicReader implements DataSourceReader {
    private DataSourceOptions options;
    ObjectNode apiObj;
    static String apiName;
    static String scriptPath;
    static String apiPath;
    OutputEndpoint outputCaller;
    DatabaseClient db;
    Map<String, String> map;
    public MarkLogicReader(DataSourceOptions options){
        this.options = options;
        this.map = options.asMap();
        apiName = "listForestNames.api";
    }

    @Override
    public StructType readSchema() {

        return new StructType(new StructField[]{
                new StructField("docNum", DataTypes.IntegerType,false, new MetadataBuilder().build()),
                new StructField("docName", DataTypes.StringType,false, new MetadataBuilder().build())});
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> list = new ArrayList<>();
        try {
            db = DatabaseClientFactory.newClient(map.get("host"), Integer.valueOf(map.get("port")),
                    new DatabaseClientFactory.DigestAuthContext(map.get("user"), map.get("password")),
                    Common.CONNECTION_TYPE);
            apiObj = IOTestUtil.readApi(apiName);
            scriptPath = IOTestUtil.getScriptPath(apiObj);
            apiPath = IOTestUtil.getApiPath(scriptPath);
            IOTestUtil.load(apiName, apiObj, scriptPath, apiPath);
            outputCaller = OutputEndpoint.on(db, new JacksonHandle(apiObj));
            Object[] forests = IOTestUtil.mapper.readValue(outputCaller.call()[0], Object[].class);

            for(Object i:forests) {
                list.add(new MarkLogicInputPartition(i.toString(), options));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return list;
    }
}
