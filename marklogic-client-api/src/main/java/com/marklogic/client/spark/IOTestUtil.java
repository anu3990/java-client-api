/*
 * Copyright 2019 MarkLogic Corporation
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
package com.marklogic.client.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class IOTestUtil {
    public final static String TEST_DIR = "data";

    public static DatabaseClient db ;
    public static DatabaseClient modDb;
    public static DocumentMetadataHandle scriptMeta;
    public static DocumentMetadataHandle docMeta ;
    public static TextDocumentManager modMgr;
    public DatabaseClient db1;

    public final static ObjectMapper mapper = new ObjectMapper();

    public IOTestUtil(String hostip, int port, String username, String password, String moduleDatabase) {
        try {
            Common common = new Common(hostip);


            X509TrustManager trustManager = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }
                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };
            SSLContext sslContext = null;
            try{
                sslContext= SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, new TrustManager[]{trustManager}, null);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            db = DatabaseClientFactory.newClient(hostip, port,  new DatabaseClientFactory.BasicAuthContext(username, password).withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY).withSSLContext(sslContext, trustManager),
                    DatabaseClient.ConnectionType.GATEWAY);
            modDb = DatabaseClientFactory.newClient(hostip, port, moduleDatabase, new DatabaseClientFactory.BasicAuthContext(username, password).withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY).withSSLContext(sslContext, trustManager),
                    DatabaseClient.ConnectionType.GATEWAY);
            scriptMeta = initDocumentMetadata(true);
            docMeta = initDocumentMetadata(false);
            modMgr = modDb.newTextDocumentManager();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Exception occurred while creating DatabaseClient "+ex.getMessage());
        }
    }
    public DatabaseClient getDb1() {

        return this.db1;
    }

    public static DocumentMetadataHandle initDocumentMetadata(boolean isScript) {
        DocumentMetadataHandle docMeta = new DocumentMetadataHandle();
        addRestDocPerms(docMeta, isScript);
        return docMeta;
    }
    public static void addRestDocPerms(DocumentMetadataHandle docMeta, boolean isScript) {
        addRestDocPerms(docMeta, "rest-reader", isScript);
        addRestDocPerms(docMeta, "rest-writer", isScript);
    }
    public static void addRestDocPerms(DocumentMetadataHandle docMeta, String role, boolean isScript) {
        boolean readOnly = role.endsWith("-reader");
        int max = readOnly ? 1 : 2;
        if (isScript)
            max++;
        DocumentMetadataHandle.Capability[] capabilities = new DocumentMetadataHandle.Capability[max];
        capabilities[0] = DocumentMetadataHandle.Capability.READ;
        if (!readOnly)
            capabilities[1] = DocumentMetadataHandle.Capability.UPDATE;
        if (isScript)
            capabilities[capabilities.length - 1] = DocumentMetadataHandle.Capability.EXECUTE;
        docMeta.getPermissions().add(role, capabilities);
    }
    public static ObjectNode readApi(String apiName) throws IOException {
        String apiBody = Common.testFileToString(TEST_DIR+File.separator+apiName);
        return mapper.readValue(apiBody, ObjectNode.class);
    }
    public static String getScriptPath(JsonNode apiObj) {
        return apiObj.get("endpoint").asText();
    }
    public static String getApiPath(String endpointPath) {
        return endpointPath.substring(0, endpointPath.length() - 3)+"api";
    }
    public static void load(String apiName, ObjectNode apiObj, String scriptPath, String apiPath) throws IOException {
        String scriptName = scriptPath.substring(scriptPath.length() - apiName.length());
        String scriptBody = Common.testFileToString(TEST_DIR+File.separator+scriptName);
        DocumentWriteSet writeSet = modMgr.newWriteSet();
        writeSet.add(apiPath,    docMeta,    new JacksonHandle(apiObj));
        writeSet.add(scriptPath, scriptMeta, new StringHandle(scriptBody));
        modMgr.write(writeSet);
    }

    static InputStream[] asInputStreamArray(String... values) {
        InputStream[] list = new InputStream[values.length];
        for (int i=0; i < values.length; i++)
            list[i] = asInputStream(values[i]);
        return list;
    }

    public static InputStream asInputStream(String value) {
        return new ByteArrayInputStream(value.getBytes());
    }

    static void writeDocuments(int count, String collection) {
        JSONDocumentManager manager = IOTestUtil.db.newJSONDocumentManager();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(collection);

        for(int i=1;i<=count;i++) {
            StringHandle data = new StringHandle("{\"docNum\":"+i+", \"docName\":\"doc"+i+"\"}");
            manager.write("/test"+i+".json", metadata, data);
        }
    }
}
