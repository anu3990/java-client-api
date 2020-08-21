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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.dataservices.InputEndpoint;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.spark.IOTestUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
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
            IOTestUtil ioTestUtil = new IOTestUtil(map.get("host"), Integer.valueOf(map.get("port")), map.get("user"),
                    map.get("password"), map.get("moduledatabase"));
            String endpointState = "{\"next\":" + 0 + ", \"prefix\":\""+map.get("prefixvalue")+"\"}";
            InputEndpoint loadEndpt = InputEndpoint.on(IOTestUtil.db, IOTestUtil.modDb.newTextDocumentManager().read("/data-hub/5/data-services/ingestion/bulkIngester.api", new StringHandle()));
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

        records.add(prepareData1(record));
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
                case "":
                    stringBuffer.append("");
                    break;
                default:
                    stringBuffer.append("\"" + record.get(fieldNumber, structField.dataType()) + "\"");
            }
        }
        return stringBuffer.toString();
    }

    private String prepareData1(InternalRow record) {

        StringWriter jsonObjectWriter = new StringWriter();

        scala.collection.immutable.Map<String,String> emptyMap = scala.collection.immutable.Map$.MODULE$.empty();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
                schema,
                jsonObjectWriter,
                new JSONOptions(emptyMap, DateTimeUtils.TimeZoneUTC().getID(), "")
        );
        jacksonGenerator.write(record);
        jacksonGenerator.flush();
        return jsonObjectWriter.toString();
    }

    private String prepareData2(InternalRow record, StructField structField, int fieldNumber) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();



        if(structField.dataType().toString().contains("StructType")) {
            // stringBuffer.append("\""+structField.name()+ "\":");
            // stringBuffer.append("{");
            stringBuffer.append("\""+structField.name()+ "\":");
            stringBuffer.append("{");
            StructType sc1 = (StructType) (structField.dataType());
            for(int k=0;k<sc1.fields().length; k++) {
                stringBuffer.append(prepareData2(record.getStruct(fieldNumber, sc1.fields().length), sc1.fields()[k], k));
                if(k!=sc1.fields().length-1)
                    stringBuffer.append(", ");
            }
            stringBuffer.append("}");

        }
        else {
            String dataType = structField.dataType().toString();
            JsonFactory factory = new JsonFactory();
            StringWriter jsonObjectWriter = new StringWriter();
            JsonGenerator generator = factory.createGenerator(jsonObjectWriter);
            generator.writeStartObject();
            generator.writeFieldName(structField.name());

                switch (dataType) {
                    case "":
                        generator.writeString("");
                        break;
                    case "BinaryType":
                        generator.writeBinary((byte[]) record.get(fieldNumber, structField.dataType()));
                        break;
                    case "BooleanType":
                        generator.writeBoolean(record.getBoolean(fieldNumber));
                        break;
                    case "DoubleType":
                        generator.writeNumber(record.getDouble(fieldNumber));
                        break;
                    case "FloatType":
                        generator.writeNumber(record.getFloat(fieldNumber));
                        break;
                    case "IntegerType":
                        generator.writeNumber(record.getInt(fieldNumber));
                        break;
                    case "LongType":
                        generator.writeNumber(record.getLong(fieldNumber));
                        break;
                    case "StringType":
                        System.out.println(record.get(fieldNumber, structField.dataType()).toString());
                        generator.writeString(String.valueOf(record.get(fieldNumber, structField.dataType())));
                        break;
                    //case "DateType" :
                    // generator.writeNumber(String.valueOf(DateTimeUtils.toJavaDate(record.getInt(fieldNumber))));  ;
                    default:
                        //DateTimeUtils.TimestampParser timestampParser = new DateTimeUtils.TimestampParser();
                        // String timestampString = timestampParser.format(record.getLong(fieldNumber));
                        generator.writeRawValue(String.valueOf(record.get(fieldNumber, structField.dataType())));
                        break;


                    // generator.writeString(record.get(fieldNumber, structField.dataType()).toString());
                }
                generator.writeEndObject();
                generator.close();
                String result = jsonObjectWriter.toString();
                stringBuffer.append(result, result.indexOf('{'), result.indexOf('}'));
            }

        //stringBuffer.append(jsonObjectWriter.toString());
        return stringBuffer.toString();
    }
}
