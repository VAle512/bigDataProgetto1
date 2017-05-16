package it.uniroma3.sparx.bigDataProgetto1.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TopProductsPerUser implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final int MAX_TOP_PRODUCTS_PER_USER = 10;
	private static final int PRODUCT_ID = 1;
	private static final int USER_ID = 2;
	private static final int SCORE = 6;

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		new TopProductsPerUser().run(args[0], args[1]);;
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("TEMPO TRASCORSO = "+elapsed/10000+" secondi.");
	}

	private void run(String inputPath, String outputPath) {

		SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName());
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> input = jsc.textFile(inputPath+"*.csv", 1);

		input.mapToPair(row -> this.splitRow(row))
		.groupByKey()
		.mapToPair(row -> this.topProducts(row))
		.sortByKey()
		.saveAsTextFile(outputPath);

		jsc.stop();
		jsc.close();
	}

	private Tuple2<String, String> topProducts(Tuple2<String, Iterable<Tuple2<String, Integer>>> row) {
		Map<String, Integer>products2Score = new HashMap<String,Integer>();
		for(Tuple2<String, Integer> tuple : row._2)
			products2Score.put(tuple._1, tuple._2);
		Map<String, Integer> orderedMap = this.mapOrderer(products2Score);
		int productsCounter = 0;
		String out = "";
		for(Map.Entry<String, Integer> entry : orderedMap.entrySet()){
			if(productsCounter == MAX_TOP_PRODUCTS_PER_USER)
				break;
			out += " " + entry.getKey() + " " + entry.getValue();
			productsCounter++;
		}		
		return new Tuple2<>(row._1,out);
	}
	
	private Map<String, Integer> mapOrderer(Map<String,Integer> map) {
		Map<String, Integer> orderedMap = new LinkedHashMap<>(); 
		orderedMap.entrySet()
		.parallelStream()
		.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
		.forEachOrdered(e -> orderedMap.put(e.getKey(), e.getValue()));
		return orderedMap;
	}

	private Tuple2<String, Tuple2<String, Integer>> splitRow(String row) {
		String[] fields = row.toString().split("\t");
		String productID = fields[PRODUCT_ID]; 
		String userID = fields[USER_ID]; 
		int score = Integer.parseInt(fields[SCORE]);
		return new Tuple2<>(userID,new Tuple2<>(productID,score));
	}

}
