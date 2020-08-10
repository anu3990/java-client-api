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

'use strict';

const state  = fn.head(xdmp.fromJSON(endpointState));
const work = fn.head(xdmp.fromJSON(workUnit));
const limit = work.limit;
const forestName = work.forestName;

const res = [];
const query = cts.collectionQuery("bulkOutputTest");
const docs = cts.search(query,["unfiltered","score-zero"],null,xdmp.forest(forestName));
const d = fn.subsequence(docs, state.next, limit);

res.push(state);
for (const x of d) {
res.push(x); };
state.next = state.next + limit;

const returnValue = Sequence.from(res);
console.log(returnValue);

returnValue;
