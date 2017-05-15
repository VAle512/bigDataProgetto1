package it.uniroma3.sparx.bigDataProgetto1.spark;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RelatedUsers implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final int PRODUCT_ID = 1;
	private static final int USER_ID = 2;
	private static final int SCORE = 6;
	private static final int MINIMUM_SCORE = 4;	
	
	public static void main(String[] args) {
		//long start = System.currentTimeMillis();
		new RelatedUsers().run(args[0], args[1]);;
		//long runTime = System.currentTimeMillis() - start;
		//System.out.println(runtime/1000);
	}

	private void run(String inputPath, String outputPath) {
		
		SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName());
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> input = jsc.textFile(inputPath+"*.csv", 1);
		
		//TODO completare
//		input.filter(row -> Integer.parseInt(row.split("\t")[SCORE]) >= 4)
//		.mapToPair(row -> this.splitRow(row));
	
		jsc.stop();
		jsc.close();
		
	}

	private Tuple2<String, String> splitRow(String row) {
			String[] fields = row.split("\t");
			String productID = fields[PRODUCT_ID];
			String userID = fields[USER_ID];
			return new Tuple2<>(productID,userID);	
		}
	}
