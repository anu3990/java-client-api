/*
 * Copyright 2012-2014 MarkLogic Corporation
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
package com.marklogic.client.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.Transaction;
import com.marklogic.client.admin.ExtensionLibrariesManager;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EvalTest {
    @BeforeClass
    public static void beforeClass() {
        Common.connectEval();
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    }
    @AfterClass
    public static void afterClass() {
        Common.release();
    }

    @Test
    public void evalTest1() throws ParserConfigurationException, DatatypeConfigurationException, JsonProcessingException, IOException, SAXException {
        // hello world and response determined by implicit StringHandle which registered with String.class
        ServerEvaluationCall query = Common.client.newServerEval().xquery("'hello world'");
        String response = query.evalAs(String.class);
        assertEquals("Return should be 'hello world'", "hello world", response);

        // hello world with a variable and response explicit set to StringHandle
        query = Common.client.newServerEval()
            .xquery("declare variable $planet external;" +
                    "'hello world from ' || $planet")
            .addVariable("planet", "Mars");
        StringHandle strResponse = query.eval(new StringHandle());
        assertEquals("Return should be 'hello world from Mars'", "hello world from Mars", strResponse.get());

        // accept and return each JSON variable type so use MultiPartResponsePage
        GregorianCalendar septFirst = new GregorianCalendar(TimeZone.getTimeZone("CET"));
        septFirst.set(2014, Calendar.SEPTEMBER, 1, 0, 0, 0);
        septFirst.set(Calendar.MILLISECOND, 0);
        query = Common.client.newServerEval()
            .javascript("var myString;" +
                    "var myArray;" +
                    "var myObject;" +
                    "var myBool;" +
                    "var myInteger;" +
                    "var myDouble;" +
                    "var myDate;" +
                    "xdmp.arrayValues([myString, myArray, myObject, myBool, myInteger, myDouble, myDate])")
            // String is directly supported in any EvalBuilder
            .addVariable("myString",  "Mars")
            // ArrayNode extends JSONNode which is mapped to implicitly use JacksonHandle
            .addVariableAs("myArray",   new ObjectMapper().createArrayNode().add("item1").add("item2"))
            // ObjectNode extends JSONNode which is mapped to implicitly use JacksonHandle
            .addVariableAs("myObject",  new ObjectMapper().createObjectNode().put("item1", "value1"))
            // the rest are auto-boxed by EvalBuilder.addVariable(String, Number)
            .addVariable("myBool",    true)
            .addVariable("myInteger", 123)
            .addVariable("myDouble",  1.1)
            .addVariable("myDate", 
                DatatypeFactory.newInstance().newXMLGregorianCalendar(septFirst).toString());
        EvalResultIterator results = query.eval();
        try {
            assertEquals("myString looks wrong", "Mars", results.next().getAs(String.class));
            assertEquals("myArray looks wrong", 
                new ObjectMapper().readTree("[\"item1\",\"item2\"]"), 
                results.next().getAs(JsonNode.class));
            assertEquals("myObject looks wrong", 
                new ObjectMapper().readTree("{\"item1\":\"value1\"}"), 
                results.next().getAs(JsonNode.class));
            assertEquals("myBool looks wrong", true, results.next().getBoolean());
            assertEquals("myInteger looks wrong", 123, 
                results.next().getNumber().intValue());
            assertEquals("myDouble looks wrong", 1.1, 
                results.next().getNumber().doubleValue(), .001);
            // the same format we sent in (from javax.xml.datatype.XMLGregorianCalendar.toString())
            assertEquals("myDate looks wrong", "2014-09-01T00:00:00.000+02:00",
              results.next().getString());
      } finally { results.close(); }

        // accept and return each XML variable type so use MultiPartResponsePage
        InputStreamHandle xquery = new InputStreamHandle(
            this.getClass().getClassLoader().getResourceAsStream("evaltest.xqy"));
        // first read it locally and run it as ad-hoc eval
        runAndTest( Common.client.newServerEval().xquery(xquery) );

        // run the same code, this time as a module we'll invoke
        xquery = new InputStreamHandle(
            this.getClass().getClassLoader().getResourceAsStream("evaltest.xqy"));
        xquery.setFormat(Format.TEXT);
        Common.connectAdmin();
        ExtensionLibrariesManager libraries = 
            Common.client.newServerConfigManager().newExtensionLibrariesManager();
        libraries.write("/ext/test/evaltest.xqy", xquery);
        Common.connectEval();
        runAndTest( Common.client.newServerEval().modulePath("/ext/test/evaltest.xqy") );
        Common.connectAdmin();
        libraries.delete("/ext/test/evaltest.xqy");
        Common.connectEval();
    }

    private void runAndTest(ServerEvaluationCall call) 
            throws JsonProcessingException, IOException, SAXException, ParserConfigurationException, DatatypeConfigurationException 
    {
        GregorianCalendar septFirst = new GregorianCalendar(TimeZone.getTimeZone("CET"));
        septFirst.set(2014, Calendar.SEPTEMBER, 1, 0, 0, 0);
        septFirst.set(Calendar.MILLISECOND, 0);

        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder()
            .parse(this.getClass().getClassLoader().getResourceAsStream("1-empty-1.0.xml"));
        call = call.addNamespace("myPrefix", "http://marklogic.com/test")
            .addVariable("myPrefix:myString",  "Mars")
            .addVariable("myArray",   
                new JacksonHandle().with(new ObjectMapper().createArrayNode().add("item1").add("item2")))
            .addVariable("myObject",  
                new JacksonHandle().with(new ObjectMapper().createObjectNode().put("item1", "value1")))
            .addVariable("myAnyUri",  "http://marklogic.com/a")
            .addVariable("myBinary",  DatatypeConverter.printHexBinary(",".getBytes()))
            .addVariable("myBase64Binary",  DatatypeConverter.printBase64Binary(new byte[] {1, 2, 3}))
            .addVariable("myHexBinary",  DatatypeConverter.printHexBinary(new byte[] {1, 2, 3}))
            .addVariable("myDuration", "P100D")
            .addVariable("myDocument", new DOMHandle(document))
            .addVariable("myQName", "myPrefix:a")
            //.addVariable("myAttribute", "<a a=\"a\"/>")
            .addVariable("myComment", "<!--a-->")
            .addVariable("myElement", "<a a=\"a\"/>")
            .addVariable("myProcessingInstruction", "<?a?>")
            .addVariable("myText", "a")
            // the next three use built-in methods of ServerEvaluationCall
            .addVariable("myBool",    true)
            .addVariable("myInteger", 1234567890123456789l)
            .addVariable("myBigInteger", "123456789012345678901234567890")
            .addVariable("myDecimal",  "1111111111111111111.9999999999")
            .addVariable("myDouble",  11111111111111111111.7777777777)
            .addVariable("myFloat",  1.1)
            .addVariable("myGDay", "---01")
            .addVariable("myGMonth", "--01")
            .addVariable("myGMonthDay", "--01-01")
            .addVariable("myGYear", "1901")
            .addVariable("myGYearMonth", "1901-01")
            .addVariable("myDate", "2014-09-01")
            .addVariable("myDateTime", 
                DatatypeFactory.newInstance().newXMLGregorianCalendar(septFirst).toString())
            .addVariable("myTime", "00:01:01")
            .addVariable("myNull", (String) null);
        EvalResultIterator results = call.eval();
        try {
            EvalResult result = results.next();
            assertEquals("myString should = 'Mars'", "Mars", result.getAs(String.class));
            assertEquals("myString should be Type.STRING", EvalResult.Type.STRING, result.getType());
            assertEquals("myString should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myArray should = [\"item1\",\"item2\"]", 
                new ObjectMapper().readTree("[\"item1\",\"item2\"]"), 
                result.getAs(JsonNode.class));
            assertEquals("myArray should be Type.JSON", EvalResult.Type.JSON, result.getType());
            assertEquals("myArray should be Format.JSON", Format.JSON, result.getFormat());
            result = results.next();
            assertEquals("myObject should = {\"item1\":\"value1\"}", 
                new ObjectMapper().readTree("{\"item1\":\"value1\"}"), 
                result.getAs(JsonNode.class));
            assertEquals("myObject should be Type.JSON", EvalResult.Type.JSON, result.getType());
            assertEquals("myObject should be Format.JSON", Format.JSON, result.getFormat());
            result = results.next();
            assertEquals("myAnyUri looks wrong", "http://marklogic.com/a", result.getString());
            assertEquals("myAnyUri should be Type.ANYURI", EvalResult.Type.ANYURI, result.getType());
            assertEquals("myAnyUri should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myBinary looks wrong", ",", result.getString());
            assertEquals("myBinary should be Type.BINARY", EvalResult.Type.BINARY, result.getType());
            assertEquals("myBinary should be Format.UNKNOWN", Format.UNKNOWN, result.getFormat());
            result = results.next();
            assertArrayEquals("myBase64Binary should = 1, 2, 3", new byte[] {1, 2, 3}, 
                DatatypeConverter.parseBase64Binary(result.getString()));
            assertEquals("myBase64Binary should be Type.BASE64BINARY", EvalResult.Type.BASE64BINARY, result.getType());
            assertEquals("myBase64Binary should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertArrayEquals("myHexBinary should = 1, 2, 3", new byte[] {1, 2, 3}, 
                DatatypeConverter.parseHexBinary(result.getString()));
            assertEquals("myHexBinary should be Type.HEXBINARY", EvalResult.Type.HEXBINARY, result.getType());
            assertEquals("myHexBinary should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myDuration should = P100D", "P100D", result.getString());
            assertEquals("myDuration should be Type.DURATION", EvalResult.Type.DURATION, result.getType());
            assertEquals("myDuration should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myQName doesn't look right", "myPrefix:a", result.getString());
            assertEquals("myQName should be Type.QNAME", EvalResult.Type.QNAME, result.getType());
            assertEquals("myQName should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myDocument doesn't look right", 
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<search:options xmlns:search=\"http://marklogic.com/appservices/search\"/>", 
                    result.getString());
            assertEquals("myDocument should be Type.XML", EvalResult.Type.XML, result.getType());
            assertEquals("myDocument should be Format.XML", Format.XML, result.getFormat());
            result = results.next();
            assertEquals("myAttribute looks wrong", "a", result.getString());
            assertEquals("myAttribute should be Type.ATTRIBUTE", EvalResult.Type.ATTRIBUTE, result.getType());
            assertEquals("myAttribute should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myComment should = <!--a-->", "<!--a-->", result.getString());
            assertEquals("myComment should be Type.COMMENT", EvalResult.Type.COMMENT, result.getType());
            assertEquals("myComment should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myElement looks wrong", "<a a=\"a\"/>", result.getString());
            assertEquals("myElement should be Type.XML", EvalResult.Type.XML, result.getType());
            assertEquals("myElement should be Format.XML", Format.XML, result.getFormat());
            result = results.next();
            assertEquals("myProcessingInstruction should = <?a?>", "<?a?>", result.getString());
            assertEquals("myProcessingInstruction should be Type.PROCESSINGINSTRUCTION", 
                EvalResult.Type.PROCESSINGINSTRUCTION, result.getType());
            assertEquals("myProcessingInstruction should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myText should = a", "a", result.getString());
            assertEquals("myText should be Type.TEXTNODE", EvalResult.Type.TEXTNODE, result.getType());
            assertEquals("myText should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myBool should = true", true, result.getBoolean());
            assertEquals("myBool should be Type.BOOLEAN", EvalResult.Type.BOOLEAN, result.getType());
            assertEquals("myBool should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myInteger should = 1234567890123456789l", 1234567890123456789l, 
                result.getNumber().longValue());
            assertEquals("myInteger should be Type.INTEGER", EvalResult.Type.INTEGER, result.getType());
            assertEquals("myInteger should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myBigInteger looks wrong", new BigInteger("123456789012345678901234567890"), 
                new BigInteger(result.getString()));
            assertEquals("myBigInteger should be Type.STRING", EvalResult.Type.STRING, result.getType());
            assertEquals("myBigInteger should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myDecimal looks wrong", 1111111111111111111.9, 
                result.getNumber().doubleValue(), .001);
            assertEquals("myDecimal should be Type.DECIMAL", EvalResult.Type.DECIMAL, result.getType());
            assertEquals("myDecimal should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myDouble looks wrong", 1.11111111111111E19, 
                result.getNumber().doubleValue(), .001);
            assertEquals("myDouble should be Type.DOUBLE", EvalResult.Type.DOUBLE, result.getType());
            assertEquals("myDouble should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myFloat looks wrong", 1.1, result.getNumber().floatValue(), .001);
            assertEquals("myFloat should be Type.FLOAT", EvalResult.Type.FLOAT, result.getType());
            assertEquals("myFloat should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myGDay looks wrong", "---01", result.getString());
            assertEquals("myGDay should be Type.GDAY", EvalResult.Type.GDAY, result.getType());
            assertEquals("myGDay should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myGMonth looks wrong", "--01", result.getString());
            assertEquals("myGMonth should be Type.GMONTH", EvalResult.Type.GMONTH, result.getType());
            assertEquals("myGMonth should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myGMonthDay looks wrong", "--01-01", result.getString());
            assertEquals("myGMonthDay should be Type.GMONTHDAY", EvalResult.Type.GMONTHDAY, result.getType());
            assertEquals("myGMonthDay should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myGYear looks wrong", "1901", result.getString());
            assertEquals("myGYear should be Type.GYEAR", EvalResult.Type.GYEAR, result.getType());
            assertEquals("myGYear should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myGYearMonth looks wrong", "1901-01", result.getString());
            assertEquals("myGYearMonth should be Type.GYEARMONTH", EvalResult.Type.GYEARMONTH, result.getType());
            assertEquals("myGYearMonth should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            // the lexical format MarkLogic uses to serialize a date
            assertEquals("myDate should = '2014-09-01", "2014-09-01", result.getString());
            assertEquals("myDate should be Type.DATE", EvalResult.Type.DATE, result.getType());
            assertEquals("myDate should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            // the lexical format MarkLogic uses to serialize a dateTime
            assertEquals("myDateTime should = '2014-09-01T00:00:00+02:00'", "2014-09-01T00:00:00+02:00", 
                result.getString());
            assertEquals("myDateTime should be Type.DATETIME", EvalResult.Type.DATETIME, result.getType());
            assertEquals("myDateTime should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myTime looks wrong", "00:01:01", result.getString());
            assertEquals("myTime should be Type.TIME", EvalResult.Type.TIME, result.getType());
            assertEquals("myTime should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myCtsQuery should be Type.OTHER", EvalResult.Type.OTHER, result.getType());
            assertEquals("myCtsQuery should be Format.TEXT", Format.TEXT, result.getFormat());
            result = results.next();
            assertEquals("myFunction should be Type.OTHER", EvalResult.Type.OTHER, result.getType());
            assertEquals("myFunction should be Format.TEXT", Format.TEXT, result.getFormat());
        } finally { results.close(); }
    }
    @Test
    public void test_171() throws Exception{
        //DatabaseClient client = Common.newEvalClient();
        DatabaseClient client = DatabaseClientFactory.newClient(
   Common.HOST, Common.PORT, "Documents", Common.EVAL_USERNAME, Common.EVAL_PASSWORD, Authentication.DIGEST);
        int count=1;
        boolean tstatus =true;
        String directory = "/test_171/";
        Transaction t1 = client.openTransaction();
        try{
            QueryManager queryMgr = client.newQueryManager();
            DeleteQueryDefinition deleteQuery = queryMgr.newDeleteDefinition();
            deleteQuery.setDirectory(directory);
            queryMgr.delete(deleteQuery);
 
            XMLDocumentManager docMgr = client.newXMLDocumentManager();
            HashMap<String,String> map= new HashMap<String,String>();
            DocumentWriteSet writeset =docMgr.newWriteSet();
            for(int i =0;i<2;i++) {
                String contents = "<xml>test" + i + "</xml>";
                String docId = directory + "sec"+i+".xml";
                writeset.add(docId, new StringHandle(contents).withFormat(Format.XML));
                map.put(docId, contents);
                if(count%100 == 0){
                    docMgr.write(writeset,t1);
                    writeset = docMgr.newWriteSet();
                }
                count++;
            }
            if(count%100 > 0){
                docMgr.write(writeset,t1);
            }
 
            QueryDefinition directoryQuery = queryMgr.newStringDefinition();
            directoryQuery.setDirectory(directory);
            SearchHandle outOfTransactionResults = queryMgr.search(directoryQuery, new SearchHandle());
 
            SearchHandle inTransactionResults = queryMgr.search(directoryQuery, new SearchHandle(), t1);
 
            assertEquals("Count of documents outside of the transaction",0,outOfTransactionResults.getTotalResults());
            assertEquals("Count of documents inside of the transaction", 2, inTransactionResults.getTotalResults());
 
        }catch(Exception e){
            System.out.println(e.getMessage());
            tstatus=true;
            throw e;
        }finally{
            if(tstatus){
                t1.rollback();
            }
        }
    } 
}
