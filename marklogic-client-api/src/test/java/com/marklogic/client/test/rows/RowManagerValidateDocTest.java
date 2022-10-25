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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowRecord;
import com.marklogic.client.test.Common;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.marklogic.client.io.Format.JSON;
import static com.marklogic.client.io.Format.XML;
import static org.junit.Assert.*;

public class RowManagerValidateDocTest extends AbstractOpticUpdateTest {

    private Set<String> expectedUris = new HashSet<>();
    private DataMovementManager dataMovementManager;

    @Before
    public void moreSetup(){
        dataMovementManager = Common.client.newDataMovementManager();
    }

    @Test
    public void xmlSchema() {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        DocumentMetadataHandle meta = newDefaultMetadata();
        dataMovementManager.startJob(writeBatcher);
        final int uriCountToWrite = 10;
        for (int i = 0; i < uriCountToWrite; i++) {
            String uri = "/acme/" + i + ".xml";
            writeBatcher.addAs(uri, meta, new StringHandle("<Doc><key>" + i + "</key><Value>value" + i + "</Value></Doc>").withFormat(XML));
            expectedUris.add(uri);
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);

        PlanBuilder.Plan plan = op
            .fromDocUris(op.cts.directoryQuery("/acme/"))
            .joinDoc(op.col("doc"), op.col("uri"))
            .validateDoc(op.col("doc"),
                op.validateDocSchemaDefinition().withKind("xmlSchema").withMode("lax"),
                op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary")
            );

        List<RowRecord> rows = resultRows(plan);
        assertEquals(uriCountToWrite, rows.size());

        XMLDocumentManager mgr = Common.client.newXMLDocumentManager();
        expectedUris.forEach(uri -> assertNotNull("URI was not written: " + uri, mgr.exists(uri)));

        // The following fails because of a bug in resultRows
//        List<String> persistedUris = rows.stream().map(row -> {
//            String uri = row.getString("uri");
//            if (StringUtils.isEmpty(uri)) {
//                fail("URI returned by resultRows is null: " + row);
//            }
//            return uri;
//        }).collect(Collectors.toList());
//
//        assertEquals(count, persistedUris.size());
//        expectedUris.forEach(uri -> assertTrue("Did not find URI: " + uri, persistedUris.contains(uri)));
    }

    @Test
    public void jsonSchema() {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        writeBatcher.addAs("/acme/jsonValidation.json", newDefaultMetadata(), new StringHandle("{\n" +
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

        PlanBuilder.ModifyPlan plan = op
            .fromDocDescriptors(
                op.docDescriptor(newWriteOp("/acme/doc1.json", mapper.createObjectNode().put("count", 1).put("total",2))),
                op.docDescriptor(newWriteOp("/acme/doc2.json", mapper.createObjectNode().put("count", 2).put("total",3)))
            )
            .validateDoc(op.col("doc"),
                op.validateDocSchemaDefinition().withKind("jsonSchema").withSchemaUri("/acme/jsonValidation.json"),
                op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary")
            );

        //verifyExportedPlanReturnsSameRowCount(plan);
        Iterator<RowRecord> rows = rowManager.resultRows(plan).iterator();
        while (rows.hasNext()){
            RowRecord str = rows.next();
            String uri = str.getString("uri");
            if(uri!=null){
                System.out.println(str.getString("doc"));
                expectedUris.add(uri);
            }
        }
        rowManager.execute(plan.write());
        DocumentManager docMgr = Common.client.newDocumentManager();
        assertTrue(docMgr.exists("/acme/doc1.json")!=null);
        assertTrue(docMgr.exists("/acme/doc2.json")!=null);

        // TODO: uncomment the below after https://bugtrack.marklogic.com/57987 is fixed
        //assertTrue(set.size()==2);
        //assertTrue(set.contains("/acme/doc1.json"));
        //assertTrue(set.contains("/acme/doc2.json"));
    }

    @Test
    public void schematron() {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher();
        writeBatcher.addAs("/acme/schematron.sch", newDefaultMetadata(), new StringHandle("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
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
        array.addObject().put("desc", "plug").put("uri", "/acme/doc2.xml");
        array.addObject().put("desc", "adaptor").put("uri", "/acme/doc1.xml");
        array.addObject().put("desc", "plug").put("uri", "/acme/doc3.xml");
        array.addObject().put("desc", "plug").put("uri", "/acme/doc4.xml");
        array.addObject().put("desc", "adaptor").put("uri", "/acme/doc5.xml");

        PlanBuilder.Plan plan = op
            .fromParam("bindingParam", "", op.colTypes(
                op.colType("uri", "string"),
                op.colType("desc", "string")
            ))
            .validateDoc(op.col("desc"),
                op.validateDocSchemaDefinition().withKind("schematron").withSchemaUri("/schematron.sch"),
                op.validateDocErrorDispositionDef().withLogSize(100).withLogLevel("summary")
            );

        plan = plan.bindParam("bindingParam", new JacksonHandle(array));

        Iterator<RowRecord> rows = rowManager.resultRows(plan).iterator();
        while (rows.hasNext()){
            RowRecord rowRecord = rows.next();
            if(rowRecord.getString("uri") != null)
                expectedUris.add(rowRecord.getString("uri"));
        }
        // TODO: uncomment the below after https://bugtrack.marklogic.com/57987 is fixed
        // assertTrue(set.size() == 5);

    }
}
