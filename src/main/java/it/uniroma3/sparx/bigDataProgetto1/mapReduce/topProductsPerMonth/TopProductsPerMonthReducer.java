package it.uniroma3.sparx.bigDataProgetto1.mapReduce.topProductsPerMonth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopProductsPerMonthReducer extends Reducer<Text, IntWritable, Text, Text> {

	private static final int MONTH_ID = 0;
	private static final int PRODUCT_ID = 1;
	private static final int TOP_PRODUCTS_PER_MONTH_NUMBER = 5;

	private Map<String, MultiValueMap> score2products = new LinkedHashMap<String, MultiValueMap>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {

		float averageScore = this.averageScore(values);

		String[] fields = key.toString().split("\t");
		String dateID = fields[MONTH_ID];
		String productID = fields[PRODUCT_ID];

		if (!this.score2products.containsKey(dateID)) {
			MultiValueMap mvMap = new MultiValueMap();
			mvMap.put(averageScore, productID);
			this.score2products.put(dateID,mvMap);
		}
		else 
			this.score2products.get(dateID).put(averageScore,productID);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String dateID : this.score2products.keySet()) {
			List<Float> scores = this.orderedScores(this.score2products.get(dateID).keySet());
			String out = this.topProducts(scores, dateID);
			//new date format yyyy-MM
			String date = dateID.substring(0, 4) + "-" + dateID.substring(4);
			context.write(new Text(date), new Text(out));
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
			Iterator<String> it = this.score2products.get(dateID).getCollection(score).iterator();
			while(it.hasNext() && topProdCounter < TOP_PRODUCTS_PER_MONTH_NUMBER)	{
				out += " ";
				out += it.next();
				out += " ";
				out += score;
				topProdCounter++;
			}
		}
		return out;
	}

}