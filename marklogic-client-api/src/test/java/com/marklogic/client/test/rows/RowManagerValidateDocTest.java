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
package com.marklogic.client.test.rows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import com.marklogic.client.test.Common;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.marklogic.client.io.Format.JSON;
import static com.marklogic.client.io.Format.XML;
import static org.junit.Assert.assertTrue;

public class RowManagerValidateDocTest {
    private final static String DIRECTORY = "/validateDocTest/";
    private static Set<String> set = new HashSet<>();
    protected ObjectMapper mapper = new ObjectMapper();
    String validateDocCollection = "RowManagerValidateDocTest";
    static DataMovementManager dataMovementManager;
    static RowManager rowManager;
    static PlanBuilder op;

    @BeforeClass
    public static void setUp(){
        Common.connect();
        dataMovementManager = Common.client.newDataMovementManager();
        rowManager = Common.client.newRowManager();
        op = rowManager.newPlanBuilder();
    }


    @Test
    public void validateDocWithxmlSchema() {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        DocumentMetadataHandle meta = new DocumentMetadataHandle().withCollections(validateDocCollection);
        dataMovementManager.startJob(writeBatcher);
        for(int i=0; i<100; i++){
            writeBatcher.addAs(DIRECTORY+i, meta, new StringHandle("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<Doc><key>"+i+"</key><Value>value"+i+"</Value></Doc>").withFormat(XML));
            set.add(DIRECTORY+i);
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
        PlanBuilder.Plan plan = op
                .fromDocUris(op.cts.directoryQuery(DIRECTORY))
                .joinDoc(op.col("doc"),op.col("uri"))
                .validateDoc(op.col("doc"),
                        op.validateDocSchemaDefinition()
                                .withKind("xmlSchema")
                        .withMode("lax"),
                        op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary"));

        Iterator<RowRecord> rows = rowManager.resultRows(plan).iterator();
        while (rows.hasNext()){
            String uri = rows.next().getString("uri");
            if(uri!=null){
                // TODO: currently the plan returns duplicates, so once removed from the set, the assert fails.
                //assertTrue(set.contains(uri));
                set.remove(uri);
            }
        }
       // TODO: uncomment the below after https://bugtrack.marklogic.com/57987 is fixed
        //assertTrue(set.size() == 0);
    }

    @Test
    public void validateDocWithFromDocDescriptor() {
        set.clear();
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        writeBatcher.addAs("/schema/jsonValidation.json", new StringHandle("{\n" +
                "       \"schema\": \"https://json-schema.org/draft/2020-12/schema\",\n" +
                "      \"id\": \"https://example.com/product.schema.json\",\n" +
                "       \"title\": \"Product\",\n" +
                "       \"description\": \"A product in the catalog\",\n" +
                "       \"type\": \"object\",\n" +
                "       \"properties\": {\n" +
                "          \"count\": { \"type\":\"integer\", \"minimum\":0 },\n" +
                "         \"total\": { \"type\":\"integer\", \"minimum\":0 },\n" +
                "          \"items\": { \"type\":\"array\", \"items\": {\"type\":\"string\", \"minLength\":1 } }\n" +
                "        }\n" +
                "     }").withFormat(JSON));
        writeBatcher.flushAndWait();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(validateDocCollection);;
        ObjectNode doc1 = mapper.createObjectNode().put("count", 1).put("total",2);
        ObjectNode doc2 = mapper.createObjectNode().put("count", 2).put("total",3);

        PlanBuilder.ModifyPlan plan = op.fromDocDescriptors(
                op.docDescriptor(
                        new DocumentWriteOperationImpl("/validateDoc/doc1.json", metadata, new JacksonHandle(doc1))),
                op.docDescriptor(
                        new DocumentWriteOperationImpl("/validateDoc/doc2.json", metadata, new JacksonHandle(doc2))))
                .validateDoc(op.col("doc"),
                        op.validateDocSchemaDefinition()
                                .withKind("jsonSchema")
                                .withSchemaUri("/schema/jsonValidation.json"),
                        op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary"));
        //verifyExportedPlanReturnsSameRowCount(plan);
        Iterator<RowRecord> rows = rowManager.resultRows(plan).iterator();
        while (rows.hasNext()){
            RowRecord str = rows.next();
            String uri = str.getString("uri");
            if(uri!=null){
                System.out.println(str.getString("doc"));
                set.add(uri);
            }
        }
        rowManager.execute(plan.write());
        DocumentManager docMgr = Common.client.newDocumentManager();
        assertTrue(docMgr.exists("/validateDoc/doc1.json")!=null);
        assertTrue(docMgr.exists("/validateDoc/doc2.json")!=null);

        // TODO: uncomment the below after https://bugtrack.marklogic.com/57987 is fixed
        //assertTrue(set.size()==2);
        //assertTrue(set.contains("/validateDoc/doc1.json"));
        //assertTrue(set.contains("/validateDoc/doc2.json"));
    }

    @Test
    public void validateDocWithSchematron() {
        set.clear();
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        writeBatcher.addAs("/schematron.sch", new StringHandle("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sch:schema xmlns:sch=\"http://purl.oclc.org/dsdl/schematron\">\n" +
                "  <sch:phase id=\"p1\">\n" +
                "    <sch:active pattern=\"pt1\"/>\n" +
                "  </sch:phase>\n" +
                "  <sch:pattern id=\"pt1\">\n" +
                "    <sch:rule context=\"Person\">\n" +
                "      <sch:assert test=\"@Title\">The element Person must have a Title attribute\n" +
                "\t\t</sch:assert>\n" +
                "      <sch:assert test=\"count(*) = 2 and count(Name) = 1 and count(Gender) = 1\">The element Person should have the child elements Name and Gender.\n" +
                "\n" +
                "\t\t</sch:assert>\n" +
                "      <sch:assert test=\"*[1] = Name\">The element Name must appear before element Gender.\n" +
                "\t\t</sch:assert>\n" +
                "    </sch:rule>\n" +
                "  </sch:pattern>\n" +
                "</sch:schema>").withFormat(XML));
        writeBatcher.flushAndWait();

        // Build the rows to bind to the plan
        ArrayNode array = mapper.createArrayNode();
        array.addObject().put("desc", "plug").put("uri", "/validateDoc/doc2.xml");
        array.addObject().put("desc", "adaptor").put("uri", "/validateDoc/doc1.xml");
        array.addObject().put("desc", "plug").put("uri", "/validateDoc/doc3.xml");
        array.addObject().put("desc", "plug").put("uri", "/validateDoc/doc4.xml");
        array.addObject().put("desc", "adaptor").put("uri", "/validateDoc/doc5.xml");
        PlanBuilder.Plan plan = op.fromParam("bindingParam", "", op.colTypes(
                op.colType("uri", "string"),
                op.colType("desc", "string")
        ))
                .validateDoc(op.col("desc"),
                        op.validateDocSchemaDefinition()
                                .withKind("schematron")
                                .withSchemaUri("/schematron.sch"),
                        op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary"));;

        plan = plan.bindParam("bindingParam", new JacksonHandle(array), null);

        Iterator<RowRecord> rows = rowManager.resultRows(plan).iterator();
        while (rows.hasNext()){
            RowRecord rowRecord = rows.next();
            if(rowRecord.getString("uri") != null)
                set.add(rowRecord.getString("uri"));
        }
        // TODO: uncomment the below after https://bugtrack.marklogic.com/57987 is fixed
        // assertTrue(set.size() == 5);

    }

    @After
    public void cleanup(){
        DocumentManager docMgr = Common.client.newDocumentManager();
        docMgr.delete("/schema/jsonValidation.json", "/schematron.sch");
        QueryManager queryMgr = Common.client.newQueryManager();
        DeleteQueryDefinition deleteQuery = queryMgr.newDeleteDefinition();
        deleteQuery.setCollections(validateDocCollection);
        queryMgr.delete(deleteQuery);
    }
}
