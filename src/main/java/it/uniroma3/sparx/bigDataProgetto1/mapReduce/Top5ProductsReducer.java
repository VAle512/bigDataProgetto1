package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5ProductsReducer extends Reducer<Text, IntWritable, Text, Text> {

	private static final int MONTH_ID = 0;
	private static final int PRODUCT_ID = 1;
	private static final int TOP_PRODUCTS_PER_MONTH_NUMBER = 5;

	private HashMap<String, MultiValueMap> map = new HashMap<String, MultiValueMap>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {

		float averageScore = this.averageScore(values);

		String[] fields = key.toString().split(" ");
		String dateID = fields[MONTH_ID];
		String productID = fields[PRODUCT_ID];

		if (!map.containsKey(dateID)) {
			MultiValueMap mvMap = new MultiValueMap();
			mvMap.put(averageScore, productID);
			map.put(dateID,mvMap);
		}
		else 
			map.get(dateID).put(averageScore,productID);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String dateID : map.keySet()) {
			List<Float> scores = this.orderedScores(map.get(dateID).keySet());
			String out = this.topProducts(scores, dateID);
			context.write(new Text(dateID), new Text(out));
		}
	}

	private float averageScore(Iterable<IntWritable> values) {
		int tot = 0;
		float sum = 0;
		for(IntWritable v : values) {
			sum += v.get();
			tot++;
		}
		return sum/tot;
	}
	
	private List<Float> orderedScores(Set<Float> set) {
		ArrayList<Float> scores = new ArrayList<Float>();
		scores.addAll(set);
		scores.sort(Collections.reverseOrder());
		return scores;
	}
	
	@SuppressWarnings("unchecked")
	private String topProducts(List<Float> scores, String dateID){
		int topProdCounter = 0;
		String out = "";
		for (Float score : scores) {
			Set<String> products = new HashSet<String>() ;
			products.addAll(map.get(dateID).getCollection(score));
			for (String p : products)	{
				if(topProdCounter == TOP_PRODUCTS_PER_MONTH_NUMBER)
					return out;
				out += " ";
				out += p;
				out += " ";
				out += score;
				topProdCounter++;
			}
		}
		return out;
	}
	
}