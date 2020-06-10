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

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Map;

public class MarkLogicInputPartition implements InputPartition<InternalRow> {
    private String forestName;
    Map<String, String> map;
    public MarkLogicInputPartition(String forestName, DataSourceOptions options) {
        this.forestName = forestName;
        this.map = options.asMap();

    }
    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new MarkLogicInputPartitionReader(this.forestName, this.map);

    }
}
