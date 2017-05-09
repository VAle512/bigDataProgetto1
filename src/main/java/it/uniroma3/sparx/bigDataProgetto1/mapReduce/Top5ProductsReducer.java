package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
		String month_id = fields[MONTH_ID];
		String product_id = fields[PRODUCT_ID];

		if (!map.containsKey(month_id)) {
			MultiValueMap mvMap = new MultiValueMap();
			mvMap.put(averageScore, product_id);
			map.put(month_id,mvMap);
		}
		else 
			map.get(month_id).put(averageScore,product_id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String monthId : map.keySet()) {
			int topProdCounter = 0;
			String out = monthId;
			ArrayList<Float> list = new ArrayList<Float>();
			list.addAll(map.get(monthId).keySet());
			list.sort(Collections.reverseOrder());
			for (Float el : list) {
				if(topProdCounter == TOP_PRODUCTS_PER_MONTH_NUMBER)
					break;
				out += map.get(monthId).get(el); //Prodotto ?
				out += " ";
				out += el; //score ?
				topProdCounter++;
			}
			context.write(new Text(monthId.substring(0, 4)), new Text(out));
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

}