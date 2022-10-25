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

import com.marklogic.client.type.PlanErrorDisposition;

/**
 * Defines an errorDisposition for using with the {@code validateDoc} operator;
 */
public interface ValidateDocErrorDispositionDef extends PlanErrorDisposition {
    /**
     * Define the size of the logs.
     *
     * @param logSize - The value may be a non-negative JavaScript integer from 0 (the default, which suppresses logging)
     *    to Number.MAX_SAFE_INTEGER (9007199254740991; effectively summarizing all errors in a single entry).
     * @return - an instance of ValidateDocErrorDispositionDef
     */
    ValidateDocErrorDispositionDef withLogSize(int logSize);
    /**
     * Define the property of the error disposition object
     *
     * @param logLevel - specifies one of the following:
     *   summary – log only the document uri and error message for each error
     *   detail – log the document uri and full error
     * @return - an instance of ValidateDocErrorDispositionDef
     */
    ValidateDocErrorDispositionDef withLogLevel(String logLevel);
}
