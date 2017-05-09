package it.uniroma3.sparx.bigDataProgetto1.mapReduce;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5ProductsReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {


	private HashMap<String, MultiValueMap> map = new HashMap<String, MultiValueMap>();



	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {

		int tot = 0;
		float sum = 0;

		for(IntWritable v : values){
			sum += v.get();
			tot++;
		}
		float average = sum/tot;

		String[] fields = key.toString().split(" ");

		if (!map.containsKey(fields[0])) {
			MultiValueMap map2 = new MultiValueMap();
			map2.put(average, fields[1]);
			map.put(fields[0],map2);
		}

		else 
			map.get(fields[0]).put(average,fields[1]);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void cleanup(Context context) {

		for (String key : map.keySet()) {
			ArrayList<Float> list = new ArrayList<Float>();
			list.addAll(map.get(key).keySet());
			list.sort(Collections.reverseOrder());
			//			for (int i=0; i<5; i++)	{
			//				String out =  (String) map.get(key).get(list.get(i));
			//				context.write(key.substring(4),);
			//TODO ciclare sui primi 5 prodotti
		}



	}

}