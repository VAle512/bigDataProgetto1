package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10FavouriteReducer extends Reducer<Text, Text, Text, Text> {
	
	private static final int TOP_PRODUCTS_PER_USER_NUMBER = 10;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		Map<String, Integer>productsAndScore = this.values2map(values);
		Map<String,Integer> sortedMap = this.mapOrderer(productsAndScore);

		int i = 0;
		String out = "";
		for(Map.Entry<String, Integer> e : sortedMap.entrySet()){
			if(i == TOP_PRODUCTS_PER_USER_NUMBER)
				break;
			out += e.getKey() + " " + e.getValue() + " ";
			i++;
		}
		context.write(key, new Text(out));
	}

	private Map<String,Integer> values2map(Iterable<Text> values) {
		Map<String, Integer>productsAndScore = new HashMap<String,Integer>();

		for (Text value : values){
			StringTokenizer st = new StringTokenizer(value.toString(), "\t");
			String prodID = st.nextToken();
			int score = Integer.parseInt(st.nextToken());
			productsAndScore.put(prodID, score);
		}
		return productsAndScore;
	}

	private Map<String,Integer> mapOrderer(Map<String,Integer> map) {
		Map <String,Integer> orderedMap = new LinkedHashMap<>();
		map.entrySet()
		.stream()
		.sorted(Map.Entry.comparingByValue())
		.forEachOrdered(x -> orderedMap.put(x.getKey(), x.getValue()));
		return orderedMap;
	}
}
