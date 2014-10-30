package com.marklogic.javaclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Calendar;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.DocumentManager.Metadata;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.DocumentMetadataHandle.Capability;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.pojo.PojoRepository;
import com.marklogic.client.pojo.annotation.Id;

/*
 * This class has test methods that check POJO's ability to read and report errors
 * (negative test cases) when the POJO class has missing @Id on its data field
 * and getter /setter methods.
 * 
 *   NOTE: If this class is renamed, then variable docId (URI) of the POJO documents needs to changed accordingly.
 *   NOTE: Also change the type value present within the json string to proper class name.
 */
public class TestPOJOMissingIdGetSetMethod extends BasicJavaClientREST {

	private static String dbName = "TestPOJOMissingIdGetSetMethodDB";
	private static String[] fNames = { "TestPOJOMissingIdGetSetMethod-1" };
	private static String restServerName = "REST-Java-Client-API-Server";

	private static int restPort = 8011;
	private DatabaseClient client;

	/*
	 * This class is used as a POJO class to read documents stored as a JSON.
	 * Class member name's getter method is missing @Id method. Setter method 
	 * has the required annotation.
	 */
	public static class SmallArtifactMissingGetter {
		
		private String name;
		private long id;
		private int inventory;
		
		// Note the @Id is missing to test this condition.
		public String getName() {
			return name;
		}
		@Id
		public void setName(String name) {
			this.name = name;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getInventory() {
			return inventory;
		}

		public void setInventory(int inventory) {
			this.inventory = inventory;
		}
	}
	
	/*
	 * This class is used as a POJO class to read documents stored as a JSON.
	 * Class member name's setter method is missing @Id method. Getter method 
	 * has the required annotation.
	 */
	public static class SmallArtifactMissingSetter {
		
		private String name;
		private long id;
		private int inventory;
		
		@Id
		public String getName() {
			return name;
		}
		// Note the @Id is missing to test this condition.
		public void setName(String name) {
			this.name = name;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getInventory() {
			return inventory;
		}

		public void setInventory(int inventory) {
			this.inventory = inventory;
		}
	}
	
	/*
	 * This class is used as a POJO class to read documents stored as a JSON.
	 * Class member name's getter and setter methods are missing @Id method.
	 */
	public static class SmallArtifactMissGetSet {
		// Note the @Id is missing to test condition of having no getter and setter at all for any class members.
		private String name;
		private long id;
		private int inventory;
				
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getInventory() {
			return inventory;
		}

		public void setInventory(int inventory) {
			this.inventory = inventory;
		}
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire",
		// "debug");
		System.out.println("In setup");
		setupJavaRESTServer(dbName, fNames[0], restServerName, restPort);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("In tear down");
		tearDownJavaRESTServer(dbName, fNames, restServerName);
	}

	@Before
	public void setUp() throws Exception {
		client = DatabaseClientFactory.newClient("localhost", restPort,
				"rest-admin", "x", Authentication.DIGEST);
	}

	@After
	public void tearDown() throws Exception {
		// release client
		client.release();
	}

	public DocumentMetadataHandle setMetadata() {
		// create and initialize a handle on the meta-data
		DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
		metadataHandle.getCollections().addAll("my-collection1",
				"my-collection2");
		metadataHandle.getPermissions().add("app-user", Capability.UPDATE,
				Capability.READ);
		metadataHandle.getProperties().put("reviewed", true);
		metadataHandle.getProperties().put("myString", "foo");
		metadataHandle.getProperties().put("myInteger", 10);
		metadataHandle.getProperties().put("myDecimal", 34.56678);
		metadataHandle.getProperties().put("myCalendar",
				Calendar.getInstance().get(Calendar.YEAR));
		metadataHandle.setQuality(23);
		return metadataHandle;
	}
	
	
	/*
	 * This method is used when there is a need to validate SmallArtifactMissingGetter.
	 */
	public void validateMissingArtifactGetter(SmallArtifactMissingGetter artifact) {
		assertNotNull("Artifact object should never be Null", artifact);
		assertNotNull("Id should never be Null", artifact.id);
		assertEquals("Id of the object is ", -99, artifact.getId());
		assertEquals("Name of the object is ", "SmallArtifact",
				artifact.getName());
		assertEquals("Inventory of the object is ", 1000,
				artifact.getInventory());
	} 
	
	/*
	 * This method is used when there is a need to validate SmallArtifactMissingSetter.
	 */
	public void validateMissingArtifactSetter(SmallArtifactMissingSetter artifact) {
		assertNotNull("Artifact object should never be Null", artifact);
		assertNotNull("Id should never be Null", artifact.id);
		assertEquals("Id of the object is ", -99, artifact.getId());
		assertEquals("Name of the object is ", "SmallArtifact",
				artifact.getName());
		assertEquals("Inventory of the object is ", 1000,
				artifact.getInventory());
	}
	
	/*
	 * This method is used when there is a need to validate SmallArtifactMissGetSet.
	 */
	public void validateMissingArtifactGetSet(SmallArtifactMissGetSet artifact) {
		assertNotNull("Artifact object should never be Null", artifact);
		assertNotNull("Id should never be Null", artifact.id);
		assertEquals("Id of the object is ", -99, artifact.getId());
		assertEquals("Name of the object is ", "SmallArtifact",
				artifact.getName());
		assertEquals("Inventory of the object is ", 1000,
				artifact.getInventory());
	} 

	/*
	 * Purpose : This test is to validate read documents stored with JacksonHandle with valid POJO
	 * specific URI. Uses SmallArtifactMissingSetter class
	 * which has @Id only on the setter method. 
	 * 
	 * Current results (10/13/2014) are: read returns a null
	 * 	 
	 */

	@Test
	public void testPOJOReadMissingSetter() throws Exception {

		String docId[] = { "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter/SmallArtifactMissingSetter.json" };
		String json1 = new String(
				"{\"com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter\":"
						+ "{\"name\": \"SmallArtifact\",\"id\": -99, \"inventory\": 1000}}");
		JSONDocumentManager docMgr = client.newJSONDocumentManager();
		docMgr.setMetadataCategories(Metadata.ALL);
		DocumentWriteSet writeset = docMgr.newWriteSet();
		// put meta-data
		DocumentMetadataHandle mh = setMetadata();
		// add to POJO URI collection. 
		mh.getCollections().addAll("com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter/SmallArtifactMissingSetter.json",
								   "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter/SmallArtifactMissingSetter.json");

		ObjectMapper mapper = new ObjectMapper();

		JacksonHandle jacksonHandle1 = new JacksonHandle();

		JsonNode junkNode = mapper.readTree(json1);
		jacksonHandle1.set(junkNode);
		jacksonHandle1.withFormat(Format.JSON);

		writeset.addDefault(mh);
		writeset.add(docId[0], jacksonHandle1);

		docMgr.write(writeset);

		PojoRepository<SmallArtifactMissingSetter, String> pojoReposSmallArtifact = client
				.newPojoRepository(SmallArtifactMissingSetter.class, String.class);
		String artifactName = new String("SmallArtifact");

		SmallArtifactMissingSetter artifact1 = pojoReposSmallArtifact.read(artifactName);
		validateMissingArtifactSetter(artifact1);
	}

	/*
	 * Purpose : This test is to validate read documents stored with JacksonHandle with valid POJO
	 * specific URI. Uses SmallArtifactMissingGetter class
	 * which has @Id only on the setter method. 
	 * 
	 * Current results (10/13/2014) are: 
	 * java.lang.IllegalArgumentException: Your getter method, setName, annotated with 
	 * com.marklogic.client.pojo.annotation.Id must not require any arguments.
	 * Issue 136 might solve this also.
	 */

	@Test
	public void testPOJOReadMissingGetter() throws Exception {

		String docId[] = { "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingGetter/SmallArtifactMissingGetter.json" };
		String json1 = new String(
				"{\"com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingGetter\":"
						+ "{\"name\": \"SmallArtifact\",\"id\": -99, \"inventory\": 1000}}");
		JSONDocumentManager docMgr = client.newJSONDocumentManager();
		docMgr.setMetadataCategories(Metadata.ALL);
		DocumentWriteSet writeset = docMgr.newWriteSet();
		// put meta-data
		DocumentMetadataHandle mh = setMetadata();
		// add to POJO URI collection. 
		mh.getCollections().addAll("com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingGetter/SmallArtifactMissingGetter.json",
								   "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingGetter/SmallArtifactMissingGetter.json");

		ObjectMapper mapper = new ObjectMapper();

		JacksonHandle jacksonHandle1 = new JacksonHandle();

		JsonNode junkNode = mapper.readTree(json1);
		jacksonHandle1.set(junkNode);
		jacksonHandle1.withFormat(Format.JSON);

		writeset.addDefault(mh);
		writeset.add(docId[0], jacksonHandle1);

		docMgr.write(writeset);

		PojoRepository<SmallArtifactMissingGetter, String> pojoReposSmallArtifact = client
				.newPojoRepository(SmallArtifactMissingGetter.class, String.class);
		String artifactName = new String("SmallArtifact");

		SmallArtifactMissingGetter artifact1 = pojoReposSmallArtifact.read(artifactName);
		validateMissingArtifactGetter(artifact1);
	}
	
	/*
	 * Purpose : This test is to validate read documents with valid POJO
	 * specific URI. Uses SmallArtifactMissingGetter class
	 * which has @Id only on the setter method. 
	 * 
	 * Current results (10/13/2014) are: 
	 * java.lang.IllegalArgumentException: Your getter method, setName, annotated with 
	 * com.marklogic.client.pojo.annotation.Id must not require any arguments.
	 * Issue 136 might solve this also.
	 */
	@Test
	public void testPOJOWriteReadMissingGetter() throws Exception {
		
	PojoRepository<SmallArtifactMissingGetter, String> pojoReposSmallArtifact = client
				.newPojoRepository(SmallArtifactMissingGetter.class, String.class);
		String artifactName = new String("SmallArtifact");

		SmallArtifactMissingGetter art = new SmallArtifactMissingGetter();
		art.setId(0);
		art.setInventory(1000);
		art.setName(artifactName);
		
		// Load the object into database
		pojoReposSmallArtifact.write(art,"com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingGetter/SmallArtifactMissingGetter.json");
						
		SmallArtifactMissingGetter artifact1 = pojoReposSmallArtifact.read(artifactName);
		validateMissingArtifactGetter(artifact1);			
	}
	
	/*
	 * Purpose : This test is to validate read documents stored with valid POJO
	 * specific URI. Uses SmallArtifactMissingSetter class
	 * which has @Id only on the setter method. 
	 * 
	 * Current results (10/13/2014) are: Works fine
	 * 	 
	 */

	@Test
	public void testPOJOWriteReadMissingSetter() throws Exception {
		
		PojoRepository<SmallArtifactMissingSetter, String> pojoReposSmallArtifact = client
				.newPojoRepository(SmallArtifactMissingSetter.class, String.class);
		String artifactName = new String("SmallArtifact");

		SmallArtifactMissingSetter art = new SmallArtifactMissingSetter();
		art.setId(-99);
		art.setInventory(1000);
		art.setName(artifactName);
		
		// Load the object into database
		pojoReposSmallArtifact.write(art,"com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter/SmallArtifactMissingSetter.json");
						
		SmallArtifactMissingSetter artifact1 = pojoReposSmallArtifact.read(artifactName);
		validateMissingArtifactSetter(artifact1);	
	}
	
	/*
	 * Purpose : This test is to validate read documents stored with JacksonHandle with valid POJO
	 * specific URI. Uses SmallArtifactMissGetSet class
	 * which has no @Id on any of its class members. 
	 * 
	 * Current results (10/13/2014) are: IllegalArgumentException
	 * 	 
	 */

	@Test(expected=IllegalArgumentException.class)
	public void testPOJOReadMissingGetterSetter() throws Exception {

		String docId[] = { "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissingSetter/SmallArtifactMissGetSet.json" };
		String json1 = new String(
				"{\"com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissGetSet\":"
						+ "{\"name\": \"SmallArtifact\",\"id\": -99, \"inventory\": 1000}}");
		JSONDocumentManager docMgr = client.newJSONDocumentManager();
		docMgr.setMetadataCategories(Metadata.ALL);
		DocumentWriteSet writeset = docMgr.newWriteSet();
		// put meta-data
		DocumentMetadataHandle mh = setMetadata();
		// add to POJO URI collection. 
		mh.getCollections().addAll("com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissGetSet/SmallArtifactMissGetSet.json",
								   "com.marklogic.javaclient.TestPOJOMissingIdGetSetMethod$SmallArtifactMissGetSet/SmallArtifactMissGetSet.json");

		ObjectMapper mapper = new ObjectMapper();

		JacksonHandle jacksonHandle1 = new JacksonHandle();

		JsonNode junkNode = mapper.readTree(json1);
		jacksonHandle1.set(junkNode);
		jacksonHandle1.withFormat(Format.JSON);

		writeset.addDefault(mh);
		writeset.add(docId[0], jacksonHandle1);

		docMgr.write(writeset);

		PojoRepository<SmallArtifactMissGetSet, String> pojoReposSmallArtifact = client
				.newPojoRepository(SmallArtifactMissGetSet.class, String.class);
		String artifactName = new String("SmallArtifact");

		SmallArtifactMissGetSet artifact1 = pojoReposSmallArtifact.read(artifactName);
		validateMissingArtifactGetSet(artifact1);
	}
	
}
