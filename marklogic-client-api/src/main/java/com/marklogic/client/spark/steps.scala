    
    val dataframe = "dynamicFrame".toDF();
    val secretName = "secretName";
    val region = "region";
    val apiPath = "apiPath";
    val prefix = "/demo"
   
    val getSecretValueRequest = new GetSecretValueRequest()
                    .withSecretId(secretName);
    
    val client  = AWSSecretsManagerClientBuilder.standard()
                                    .withRegion(region)
                                    .build();
    val getSecretValueResult = client.getSecretValue(getSecretValueRequest)
    
    val secret = getSecretValueResult.getSecretString()
    
    
    val mapValue =  secret.substring(1, secret.length - 1)
        .split(",")
        .map(_.split(":"))
        .map { case Array(k, v) => (k.substring(1, k.length-1), v.substring(1, v.length-1))}
        .toMap

    val host = mapValue("host")
    val port = mapValue("port")
    val username = mapValue("username")
    val password = mapValue("password")
    
    val moduledb = mapValue("modulesdb")
    
    val writer = dataframe.write.format("com.marklogic.client.spark.Writer.MarkLogicWriteDataSource")
                    .option("host", host)
                    .option("port", port)
                    .option("user", username)
                    .option("password",password)
                    .option("prefixvalue", prefix)
                    .option("moduledatabase", moduledb)
                    .option("batchsize", 10)
                    .option("apiPath", apiPath);

    val t2 =  writer.save;
