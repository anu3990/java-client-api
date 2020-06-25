1. Setting up marklogic on an AWS instance -
    a. To install marklogic on an ec2 instance follow - https://developer.marklogic.com/products/cloud/aws/
    b. ssh into the above instance, and install git (yum install git). 
    c. git clone https://github.com/marklogic/java-client-api.git
    d. Create the databases and app servers using - ./gradlew marklogic-client-api:testServerInit 
        (If you want to create the database and app server manually please skip this step and 
        modify Common.java and IOTestUtil.java accordingly.)

2. Steps to create the client jar -
    a. Download the latest marklogic client code from - https://github.com/marklogic/java-client-api/tree/develop
    b. Place this spark folder under - marklogic-client-api/src/main/java/com/marklogic.client
    c. Copy the contents of data folder to - marklogic-client-api/src/main/resources/data
    d. In MarkLogicSparkWriteDriver.java, modify the parameters - host,port, username and password according to the previous step. 
    e. Add the host ip address to the definition of HOST in Common.java
    f. Modify the build.gradle to add dependencies mentioned in the file - build.gradle-copy in order to include all the 
        dependencies in the final jar.
    g. Build the jar (intellij/eclipse or command line)
    
3. Steps to make the spark connector available to the emr cluster -
    a. Place marklogic-client-api jar (created in the previous step) in an s3 folder.
    b. Create an EMR cluster and ssh into the master node.
    c. Copy the jar from the s3 folder into the emr master node using the following command - 
        aws s3 cp s3://file-path-on-s3 .
        
4. To run the spark write driver - 
        spark-submit --class com.marklogic.client.spark.MarkLogicSparkWriteDriver ./marklogic-client-api-5.0-SNAPSHOT.jar