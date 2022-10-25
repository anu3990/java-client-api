/*
 * Copyright (c) 2022 MarkLogic Corporation
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
package com.marklogic.client.expression;

import com.marklogic.client.type.PlanSchemaDef;

import java.util.Map;
/**
 * Defines the schema for using with the {@code validateDoc} operator;
 */
public interface ValidateDocSchemaDefinition extends PlanSchemaDef {
    /**
     * Define the kind property of the schema object
     *
     * @param kind - the kind property of the schema object must be jsonSchema, schematron, or xmlSchema.
     * @return - an instance of ValidateDocSchemaDefinition
     */
    ValidateDocSchemaDefinition withKind(String kind);
    /**
     * Defines the schema as a node object.
     *
     * @param schema - key value pair defining a schema
     * @return - an instance of ValidateDocSchemaDefinition
     */
    // TODO : add tests when https://bugtrack.marklogic.com/58025 is fixed
    ValidateDocSchemaDefinition withSchema(Map<Object, Object> schema);
    /**
     * Defines the mode property.
     *
     * @param mode - takes a strict, lax or type value
     * @return - an instance of ValidateDocSchemaDefinition
     */
    ValidateDocSchemaDefinition withMode(String mode);
    /**
     * Defines the schema based on an existing document in the database.
     *
     * @param schemaUri - property takes a string with a schematron URI
     * @return - an instance of ValidateDocSchemaDefinition
     */
    ValidateDocSchemaDefinition withSchemaUri(String schemaUri);
}
