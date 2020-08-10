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

import com.marklogic.client.spark.MarkLogicSparkWriteDriver;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

import java.util.HashMap;
import java.util.Map;

public class MarkLogicDataWriterFactory implements DataWriterFactory<InternalRow> {
    private Map<String, String> map;
    public MarkLogicDataWriterFactory(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        System.out.println("************** task id ************** "+ taskId);

        Map<String, String> mapConfig = new HashMap<>();
        mapConfig.putAll(map);
        mapConfig.put("taskId",String.valueOf(taskId));
       // MarkLogicSparkWriteDriver.taskIds.add(taskId);

        return new MarkLogicDataWriter(mapConfig);
    }
}
