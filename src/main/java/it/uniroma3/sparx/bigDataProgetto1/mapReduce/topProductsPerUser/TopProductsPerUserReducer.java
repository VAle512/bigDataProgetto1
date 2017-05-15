package it.uniroma3.sparx.bigDataProgetto1.mapReduce.topProductsPerUser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopProductsPerUserReducer extends Reducer<Text, Text, Text, Text> {
	
	private static final int MAX_TOP_PRODUCTS_PER_USER = 10;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Map<String, Integer>productsAndScore = this.values2map(values);
		Map<String,Integer> sortedMap = this.mapOrderer(productsAndScore);
		String out = this.topProducts(sortedMap);
		context.write(key, new Text(out));
	}

	private String topProducts(Map<String, Integer> map) {
		int productsCounter = 0;
		String out = "";
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(productsCounter == MAX_TOP_PRODUCTS_PER_USER)
				break;
			out += entry.getKey() + " " + entry.getValue() + " ";
			productsCounter++;
		}
		return out;
	}
	
	private Map<String,Integer> mapOrderer(Map<String,Integer> map) {
		Map <String,Integer> orderedMap = new LinkedHashMap<>();
		map.entrySet()
		.parallelStream()
		.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
		.forEachOrdered(x -> orderedMap.put(x.getKey(), x.getValue()));
		return orderedMap;
	}

	private Map<String,Integer> values2map(Iterable<Text> values) {
		Map<String, Integer>products2Score = new HashMap<String,Integer>();
		for (Text value : values){
			StringTokenizer st = new StringTokenizer(value.toString(), "\t");
			String prodID = st.nextToken();
			int score = Integer.parseInt(st.nextToken());
			products2Score.put(prodID, score);
		}
		return products2Score;
	}
}
