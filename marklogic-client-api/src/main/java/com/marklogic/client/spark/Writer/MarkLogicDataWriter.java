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
package com.marklogic.client.spark.Writer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.MarkLogicInternalException;
import com.marklogic.client.dataservices.InputEndpoint;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.spark.Common;
import com.marklogic.client.spark.IOTestUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MarkLogicDataWriter implements DataWriter<InternalRow> {
    static String scriptPath;
    static String apiPath;
    static ObjectNode apiObj;
    private Map<String, String> map;
    private List<String> records;
    InputEndpoint.BulkInputCaller loader;
    String[] headers;
    private int taskId;

    DatabaseClient db;

    public MarkLogicDataWriter(Map<String, String> map) {
        try {
            this.map = map;
            this.records = new ArrayList<>();
            this.taskId = Integer.valueOf(map.get("taskId"));
            headers = map.get("schema").split(",");
            String apiName = "bulkInputCallerImpl.api";
            db = DatabaseClientFactory.newClient(map.get("host"), Integer.valueOf(map.get("port")),
                    new DatabaseClientFactory.DigestAuthContext(map.get("user"), map.get("password")),
                    Common.CONNECTION_TYPE);
            apiObj = IOTestUtil.readApi(apiName);
            scriptPath = IOTestUtil.getScriptPath(apiObj);
            apiPath = IOTestUtil.getApiPath(scriptPath);
            IOTestUtil.load(apiName, apiObj, scriptPath, apiPath);

            String endpointState = "{\"next\":" + 0 + "}";
            InputEndpoint loadEndpt = InputEndpoint.on(IOTestUtil.db, new JacksonHandle(apiObj));

            this.loader = loadEndpt.bulkCaller();
            String workUnit = "{\"taskId\":" + taskId + "}";
            loader.setWorkUnit(new ByteArrayInputStream(workUnit.getBytes()));
            loader.setEndpointState(new ByteArrayInputStream(endpointState.getBytes()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {

        records.add("{\""+headers[0]+"\":"+record.getString(0)+", \""+headers[1]+"\":"+"\""+record.getString(1)+"\""+"}");

        if(records.size() == Integer.valueOf(map.get("batchsize"))) {
            writeRecords();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if(!this.records.isEmpty()) {
            writeRecords();
        }

        return null;
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException("Transaction cannot be aborted");
    }

    private void writeRecords() {
        System.out.println("*********** Batch size "+ records.size());
        Stream.Builder<InputStream> builder = Stream.builder();

        for(int i=0; i< records.size(); i++){
            System.out.println(records.get(i));
            builder.add(IOTestUtil.asInputStream(records.get(i)));
        }
        Stream<InputStream> input = builder.build();

        input.forEach(loader::accept);
        loader.awaitCompletion();
        taskId+= records.size();
        this.records.clear();

    }
}
