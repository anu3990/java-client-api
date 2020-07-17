import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.SparkContext
import com.marklogic.client.spark.Writer.MarkLogicWriteDataSource
import com.marklogic.client.spark._
import scala.collection.JavaConverters._

object GlueApp {
  def main(args: Array[String]) {

val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession


    val inputPath = "s3-input-path-of-docs-1-to-100.csv"
    val ip = "ipv4 address of the instance"

    val javaApi = new MarkLogicSparkWriteDriver()
    val t1 = javaApi.mlSparkDriver(inputPath, ip)

  }
}