package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelatedUsersReducer1 extends Reducer<Text, Text, Text, Text> {

	MultiValueMap map = new MultiValueMap();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> users = new LinkedList<>();
		for (Text value : values)
			users.add(value.toString());
		for(int i=0;i<users.size();i++)
			for(int j=i+1;j<users.size();j++) {
				String couple = users.get(i) + "\t" + users.get(j);
				context.write(new Text(couple), key);
			}	
	}

}