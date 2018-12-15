package com.marklogic.client.dhs.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.dhs.JobLogger;
import com.marklogic.client.dhs.JobRunner;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Demo {
   static final private Logger logger = LoggerFactory.getLogger(Demo.class);

// TODO: extracted jobId instead of hard-coded
   private final static String jobId = "9a8351a1-31bc-417b-96fa-d27525e6664d";

   public static void main(String... args) throws IOException {
//      System.out.println("hello, docker");
      new Demo().loadJSONLocally();
//      new Demo().loadJSONFromCSVOnS3();
   }

   private void loadJSONLocally() throws IOException {
      // run in Docker with --network="host"
      String hostname = System.getProperty("DB_CONNECT_HOST", "localhost");
      int port = Integer.parseInt(System.getProperty("DB_CONNECT_PORT", "8012"));
      String username = System.getProperty("DB_CONNECT_USER", "admin");
      String password = System.getProperty("DB_CONNECT_PASSWORD", "admin");

      DatabaseClient client = DatabaseClientFactory.newClient(
            hostname, port, new DatabaseClientFactory.DigestAuthContext(username, password)
      );

      String csvPath = "./data/OrderLines.csv";

      File csvFile = new File(csvPath);
      File jsonFile = new File(JobRunner.jobFileFor(csvPath));

// System.out.println("csvFile: "+csvFile.exists()+", length: "+csvFile.length());
// System.out.println("jsonFile: "+jsonFile.exists()+", length: "+jsonFile.length());

      try (
            InputStream csvStream = new FileInputStream(csvFile);
            InputStream jobStream = new FileInputStream(jsonFile);
      ) {
         JobRunner jobRunner = new JobRunner();
         jobRunner.setRoles("rest-tracer");

System.out.println("starting job");

         jobRunner.run(
               client, csvStream, jobStream, new DBLogger(client, jobId), csvFile.length()
         );

System.out.println("finished job");
      } catch(Exception ex) {
         System.out.println("error: "+ex.getMessage());
         ex.printStackTrace();
      } finally {
         client.release();
      }
   }

// TODO: environment properties for input (bucket + object) and credentials
   private void loadJSONFromCSVOnS3() throws IOException {
      String regionName = System.getProperty("REGION_NAME", "us-west-1");
      String bucketName = System.getProperty("BUCKET_NAME", "ehennum-west-test");
      String objectKey  = System.getProperty("OBJECT_KEY", "csvTest/OrderLines.csv");

// TODO: default to hostname for load balancer
      // run in Docker with --network="host"
      String hostname = System.getProperty("DB_CONNECT_HOST", "localhost");
      int port = Integer.parseInt(System.getProperty("DB_CONNECT_PORT", "8000"));
      String username = System.getProperty("DB_CONNECT_USER", "admin");
      String password = System.getProperty("DB_CONNECT_PASSWORD", "admin");

// TODO: real authentication and authorization
      AWSCredentials credentials = getCredentials();
      AmazonS3 s3Client = AmazonS3ClientBuilder
         .standard()
         .withCredentials(new AWSStaticCredentialsProvider(credentials))
         .withRegion(regionName)
         .build();

      DatabaseClient client = DatabaseClientFactory.newClient(
            hostname, port, new DatabaseClientFactory.DigestAuthContext(username, password)
      );

      S3BucketAccessor bucketAccessor = new S3BucketAccessor(s3Client, bucketName);

// TODO: status and error logger input streams
      try (
            InputStream csvStream = bucketAccessor.readObject(objectKey);
            InputStream jobStream = bucketAccessor.readObject(JobRunner.jobFileFor(objectKey));
            ) {
System.out.println("starting job");
         ObjectMetadata csvMetadata = s3Client.getObjectMetadata(bucketName, objectKey);

         JobRunner jobRunner = new JobRunner();
         jobRunner.setRoles("rest-tracer");

         jobRunner.run(
               client, csvStream, jobStream, new DBLogger(client, jobId), csvMetadata.getInstanceLength()
         );

System.out.println("finished job");
      } catch(Exception ex) {
         System.out.println("error: "+ex.getMessage());
         ex.printStackTrace();
      } finally {
         client.release();
      }
   }
   private AWSCredentials getCredentials() {
      return new BasicAWSCredentials(
            System.getProperty("AWS_ACCESS_KEY", "<ACCESS_KEY>"),
            System.getProperty("AWS_SECRET_KEY", "<SECRET_KEY>")
      );
   }

   private static class DBLogger implements JobLogger {
      private String jobId;

      private JSONDocumentManager jsonMgr;
      private TextDocumentManager textMgr;

      private DocumentMetadataHandle msgMetadata;

      private AtomicInteger counter = new AtomicInteger();

      private DBLogger(DatabaseClient client, String jobId) {
         this.jobId = jobId;

         jsonMgr = client.newJSONDocumentManager();
         textMgr = client.newTextDocumentManager();

         msgMetadata = new DocumentMetadataHandle();
         msgMetadata.withCollections(JobRunner.getJobCollection(jobId));
         msgMetadata.getPermissions().add(
               "rest-writer",
               DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE
         );
      }
      @Override
      public void status(ObjectNode msg) {
         jsonMgr.write(makeUri("json"), msgMetadata, new JacksonHandle(msg));
      }
      @Override
      public void error(String msg) {
         textMgr.write(makeUri("txt"), msgMetadata, new StringHandle(msg));
      }
      @Override
      public void error(String msg, Throwable err) {
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         PrintStream printer = new PrintStream(out);
         printer.println(msg);
         err.printStackTrace(printer);
         printer.flush();
         printer.close();
         textMgr.write(makeUri("txt"), msgMetadata, new BytesHandle(out.toByteArray()));
      }
      private String makeUri(String format) {
         return JobRunner.getJobDirectory(jobId)+"msg"+counter.incrementAndGet()+"."+format;
      }
   }
}
