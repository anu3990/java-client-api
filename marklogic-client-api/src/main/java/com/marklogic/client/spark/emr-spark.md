1. Setting up marklogic on an AWS instance -
    a. To install marklogic on an ec2 instance follow - https://developer.marklogic.com/products/cloud/aws/
    b. ssh into the above instance, and install git (yum install git). 
    c. The ec2 instance might not have jdk 1.8 installed. Please run - "sudo yum install java-1.8.0-openjdk-devel".
    d. git clone https://github.com/marklogic/java-client-api.git followed by "cd java-client-api"
    e. Since we are working with java-1.8, checkout the tag - 4.2.0 using - "git checkout tags/4.2.0" 
    f. Create the databases and app servers using - ./gradlew marklogic-client-api:testServerInit 
        (If you want to create the database and app server manually please skip steps d, e and f, and 
        populate Common.java and IOTestUtil.java accordingly.)

2. Steps to create the client jar -
    a. Download the marklogic client code from - https://github.com/anu3990/java-client-api/tree/develop (This has the latest java-client code as of 06/10/2020, spark code as well as the required changes needed 
    	due to rollback to java version 1.8)
    b. In MarkLogicSparkWriteDriver.java (path - marklogic-client-api/src/main/java/com/marklogic/client/spark/MarkLogicSparkWriteDriver.java), modify the parameters - host,port, username and password according to step-1 and 
    	add the s3 path of the input data as a parameter in .csv
    c. Add the host ip address to the definition of HOST in Common.java (path - marklogic-client-api/src/main/java/com/marklogic/client/spark/Common.java).
    d. Build the jar using intellij/eclipse or command line (For command line - goto the project folder and run - ./gradlew build -x test).
    
3. Steps to create an EMR cluster -
	a. Login to AWS console and goto - EMR.
    b. On the EMR page click on "Create Cluster" to goto quick options.
    c. Under General Configurations : Provide an appropriate name for the cluster and an S3 folder for Logging. Leave the Launch Mode as Cluster.
    d. Under Software Configuration : Select Application as - "Spark: Spark 2.4.5 on Hadoop 2.8.5 YARN and Zeppelin 0.8.2". Leave the Release version as given.
    f. Under Hardware Configuration : Select instance type as - m3.xlarge. Number of instances as 2 (one master node and one core node should be enough).
    g. Under Security and Access : Select an EC2 key-pair for secure ssh to the master node (create a new key-pair if one does not exist). Select Permissions - "Custom".
    	Select EMR Role as - "EMR_DefaultRole" (If the default role does not exist please contact admin to create one).
    h. Click on - "Create Cluster" on the bottom right.
    
4. Steps to make the spark connector available to the EMR cluster -
    a. Place marklogic-client-api jar (created in the previous step) in an s3 folder.
    b. ssh into the master node of the EMR cluster created in the previous step.
    c. Copy the jar from the s3 folder into the emr master node using the following command - 
        aws s3 cp s3://file-path-on-s3 .
        
5. To run the spark write driver - 
        spark-submit --class com.marklogic.client.spark.MarkLogicSparkWriteDriver ./marklogic-client-api-5.0-SNAPSHOT.jar
        
        
NOTE : The EMR cluster and the instance with marklogic(created in Step - 1) should all be in the same AWS region.
	   Please refer FAQ.txt in case of any exception.
