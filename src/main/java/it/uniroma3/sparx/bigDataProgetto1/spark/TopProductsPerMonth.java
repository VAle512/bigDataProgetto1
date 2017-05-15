package it.uniroma3.sparx.bigDataProgetto1.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.uniroma3.sparx.bigDataProgetto1.util.TimeConverter;
import scala.Tuple2;

public class TopProductsPerMonth implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final int TOP_PRODUCTS_PER_MONTH_NUMBER = 5;
	private static final int PRODUCT_ID = 1;
	private static final int SCORE = 6;
	private static final int TIME = 7;
	
	public static void main(String[] args) {
		//long start = System.currentTimeMillis();
		new TopProductsPerMonth().run(args[0], args[1]);;
		//long runTime = System.currentTimeMillis() - start;
		//System.out.println(runtime/1000);
	}

	private void run(String inputPath, String outputPath) {
		
		SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName());
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> input = jsc.textFile(inputPath+"*.csv", 1);
		
		input.mapToPair(row -> this.splitRow(row))
		.reduceByKey((a,b) ->  new Tuple2<Integer, Float>(a._1 + b._1, a._2 + b._2))
		.mapValues(a -> a._1 / a._2)
		.mapToPair(row -> this.changeMapping(row))
		.groupByKey()
		.mapToPair(row -> this.topProducts(row))
		.sortByKey()
		.saveAsTextFile(outputPath);
		
		jsc.stop();
		jsc.close();

	}

	@SuppressWarnings("unchecked")
	private Tuple2<String, String> topProducts(Tuple2<String, Iterable<String>> row) {
		MultiValueMap score2products = new MultiValueMap();
		for (String s : row._2) {
			StringTokenizer st = new StringTokenizer(s, "\t");
			String productID = st.nextToken();
			float score = Float.parseFloat(st.nextToken());
			score2products.put(score, productID);
		}
		List<Float> scores = new LinkedList<Float>();
		scores.addAll(score2products.keySet());
		scores.sort(Collections.reverseOrder());
		String out = this.topProducts(scores, score2products);
		return new Tuple2<>(row._1,out);
	}

	@SuppressWarnings("unchecked")
	private String topProducts(List<Float> orderedScores, MultiValueMap score2products) {
		int prodCounter = 0;
		String out = "";
		for(Float score : orderedScores) {
			Iterator<String> it = score2products.getCollection(score).iterator();
			while(it.hasNext() && prodCounter < TOP_PRODUCTS_PER_MONTH_NUMBER) {
				out += " ";
				out += it.next();
				out += " ";
				out += score;
				prodCounter++;
			}
		}
		return out;
	}

	private Tuple2<String, String> changeMapping(Tuple2<String, Float> tuple) {
		StringTokenizer st = new StringTokenizer(tuple._1, "\t");
		String monthID = st.nextToken();
		String productID = st.nextToken();
		return new Tuple2<>(monthID, productID + "\t" + tuple._2);
	}

	private Tuple2<String, Tuple2<Integer, Float>> splitRow(String row) {
		String[] fields = row.split("\t");
		String productID = fields[PRODUCT_ID];
		long time = Long.parseLong(fields[TIME]);
		int score = Integer.parseInt(fields[SCORE]);
		String monthID = TimeConverter.unix2String(time);
		return new Tuple2<>(monthID + "\t" + productID , new Tuple2<>(score, new Float(1)));	
	}

}