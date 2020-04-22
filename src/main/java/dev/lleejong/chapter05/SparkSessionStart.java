package dev.lleejong.chapter05;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class SparkSessionStart {

	public static SparkSession getSparkSession(String appName, String master) {
		return SparkSession.builder()
			.appName(appName)
			.master(master)
//			.config("spark.local.ip", "127.0.0.1")
			.getOrCreate();
	}

	public static void wordCountUsingUntypedOperation(SparkSession spark, String source){
		Dataset<Row> df = spark.read().text(source);
		Dataset<Row> wordDF = df.select(explode(split(col("value"), " ")).as("word"));
		Dataset<Row> result = wordDF.groupBy("word").count().sort();
		result.sort(desc("count")).show();
	}

	public static void wordCountUsingTypedOperation(SparkSession spark, String source){
		Dataset<Row> df = spark.read().text(source);
		Dataset<String> ds = df.as(Encoders.STRING());
		Dataset<String> wordDF = ds.flatMap(
			(String s) -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());
		Dataset<Tuple2<String, Object>> result = wordDF.groupByKey(
			(MapFunction<String, String>) value -> value, Encoders.STRING()).count();
		result.show();
	}

	public static void main(String[] args) {
		SparkSession spark = getSparkSession("Sample", "local[*]");
		String source = "file:///Users/JOLEE01/spark/spark-2.4.5-bin-hadoop2.7/README.md";
		wordCountUsingUntypedOperation(spark, source);
		wordCountUsingTypedOperation(spark, source);
//		wordCountUsingUntypedOperation(spark, source, "/Users/JOLEE01/workspace/lleejongWorkspace/SparkTutorial/out/chapter05/result/wordCount_untyped_result.txt");
//		wordCountUsingTypedOperation(spark, source, "/Users/JOLEE01/workspace/lleejongWorkspace/SparkTutorial/out/chapter05/result/wordCount_typed_result.txt");
		spark.stop();
	}



}
