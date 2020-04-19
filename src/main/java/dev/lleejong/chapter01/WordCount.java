package dev.lleejong.chapter01;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
	public static void main(String[] args) {
		if (ArrayUtils.getLength(args) != 3) {
			System.out.println("Usage: WordCount <Master> <Input> <Output>");
			return;
		}
		//Step1. SparkContext 생성
		JavaSparkContext sc = getSparkContext("WordCount", args[0]);
		JavaRDD<String> inputRDD = getInputRDD(sc, args[1]);

		//Step3: 필요한 처리를 수행
		JavaPairRDD<String, Integer> resultRDD = process(inputRDD);

		//Step4: 수행 결과 처리
		handleResult(resultRDD, args[2]);

		//Step5: Spark와의 연결 종료
		sc.stop();
	}

	public static JavaSparkContext getSparkContext(String appName, String master) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		return new JavaSparkContext(conf);
	}

	public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
		return sc.textFile(input);
	}

	public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {
		return inputRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		.mapToPair(word -> new Tuple2<>(word, 1))
		.reduceByKey(Integer::sum);
	}

	public static void handleResult(JavaPairRDD<String, Integer> resultRDD, String output){
		resultRDD.saveAsTextFile(output);
	}
}
