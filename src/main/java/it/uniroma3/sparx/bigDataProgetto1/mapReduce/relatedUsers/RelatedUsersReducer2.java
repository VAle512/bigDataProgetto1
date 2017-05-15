package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelatedUsersReducer2 extends Reducer<Text, Text, Text, Text>  {

	private static final int MIUMIN_RELATED_PRODUCTS = 3;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		String out = "";
		for(Text value : values) {
			count++;
			out +=  " " + value.toString();
		}
		if(count >= MIUMIN_RELATED_PRODUCTS)
			context.write(key, new Text(out));
	}

}
