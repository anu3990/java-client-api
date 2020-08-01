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
import com.marklogic.client.dataservices.InputEndpoint;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.spark.IOTestUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MarkLogicDataWriter implements DataWriter<InternalRow> {
    String scriptPath;
    String apiPath;
    ObjectNode apiObj;
    private Map<String, String> map;
    private List<String> records;
    InputEndpoint.BulkInputCaller loader;
    private int taskId = 1;
    private StructType schema;
    final private Logger logger = LoggerFactory.getLogger(MarkLogicDataWriter.class);

    public MarkLogicDataWriter(Map<String, String> map, StructType schema) {
        System.out.println("************ Reached MarkLogicDataWriter ****************");
        logger.info("************ Reached MarkLogicDataWriter ****************"+schema);

        try {
            this.map = map;
            this.records = new ArrayList<>();
            this.taskId = Integer.valueOf(map.get("taskId"));
            this.schema = schema;
            String apiName = "bulkInputCallerImpl.api";
            IOTestUtil ioTestUtil = new IOTestUtil(map.get("host"));
            apiObj = ioTestUtil.readApi(apiName);
            scriptPath = ioTestUtil.getScriptPath(apiObj);
            apiPath = ioTestUtil.getApiPath(scriptPath);
            ioTestUtil.load(apiName, apiObj, scriptPath, apiPath);
            String endpointState = "{\"next\":" + 0 + "}";
            InputEndpoint loadEndpt = InputEndpoint.on(ioTestUtil.db, new JacksonHandle(apiObj));

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
        System.out.println("************ Reached MarkLogicDataWriter.write **************** ");
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("{");
        for(int i=0;i<schema.fields().length; i++) {
            stringBuffer.append(prepareData(record, schema.fields()[i], i));
            if(i!=schema.fields().length-1)
                stringBuffer.append(", ");
        }

        stringBuffer.append("}");
        records.add(stringBuffer.toString());
        System.out.println(records);
        System.out.println("**** Number of fields in internal row **** "+record.numFields());

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
        System.out.println("************ Reached MarkLogicDataWriter.writeRecords ****************");

        Stream.Builder<InputStream> builder = Stream.builder();

        for(int i=0; i< records.size(); i++){
            System.out.println(records.get(i) + "***** with size **** "+ records.size());
            builder.add(IOTestUtil.asInputStream(records.get(i)));
        }
        Stream<InputStream> input = builder.build();
        input.forEach(loader::accept);
        loader.awaitCompletion();
        taskId+= records.size();
        this.records.clear();
        System.out.println("*********** After writing data ");

    }

    private String prepareData(InternalRow record, StructField structField, int fieldNumber) {
        StringBuffer stringBuffer = new StringBuffer();

        stringBuffer.append("\""+structField.name()+ "\":");
        if(structField.dataType().toString().contains("StructType")) {
            stringBuffer.append("{");
            StructType sc1 = (StructType) (structField.dataType());
            for(int k=0;k<sc1.fields().length; k++) {
                stringBuffer.append(prepareData(record.getStruct(fieldNumber, sc1.fields().length), sc1.fields()[k], k));
                if(k!=sc1.fields().length-1)
                    stringBuffer.append(", ");
            }
            stringBuffer.append("}");
        }
        else {
            String dataType = structField.dataType().toString();

            switch (dataType) {
                case "BinaryType":
                case "LongType":
                case "DoubleType":
                case "FloatType":
                case "IntegerType":
                    stringBuffer.append(record.get(fieldNumber, structField.dataType()));
                    break;
                case "BooleanType":
                    stringBuffer.append(record.getBoolean(fieldNumber));
                    break;
                default:
                    stringBuffer.append("\"" + record.get(fieldNumber, structField.dataType()) + "\"");
            }
        }
        return stringBuffer.toString();
    }
}
