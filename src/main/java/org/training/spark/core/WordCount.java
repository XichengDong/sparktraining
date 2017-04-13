package org.training.spark.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class WordCount {
  public static void main(String[] args) throws Exception {
    String masterUrl = "local[1]";
    String inputFile = "data/textfile/";
    String outputFile = "/tmp/output";

    if (args.length > 0) {
      masterUrl = args[0];
    } else if(args.length > 2) {
      inputFile = args[1];
      outputFile = args[2];
    }
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setMaster(masterUrl).setAppName("wordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputFile);

    // Split up into words.
    JavaRDD<String> words = input.flatMap(
        new FlatMapFunction<String, String>() {
          @Override
          public Iterator<String> call(String x) {
            return Arrays.asList(x.split(" ")).iterator();
          }
        }
    ).filter(
        new Function<String, Boolean>() {
          @Override
          public Boolean call(String s) throws Exception {
            return s.length() > 1;
          }
        }
    );

    // Transform into word and count.
    JavaPairRDD<String, Integer> counts = words.mapToPair(
        new PairFunction<String, String, Integer>(){
          @Override
          public Tuple2<String, Integer> call(String x){
            return new Tuple2(x, 1);
          }
        }
    ).reduceByKey(
        new Function2<Integer, Integer, Integer>(){
          public Integer call(Integer x, Integer y){
            return x + y;
          }
        }
    );

    // Save the word count back out to a text file, causing evaluation.
    Path outputPath = new Path(outputFile);
    FileSystem fs = outputPath.getFileSystem(new HdfsConfiguration());
    if(fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    counts.saveAsTextFile(outputFile);

    // Just for debugging, NOT FOR PRODUCTION
    counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
      @Override
      public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        System.out.println(String.format("%s - %d", stringIntegerTuple2._1(), stringIntegerTuple2._2()));
      }
    });
  }
}