package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelatedUsersReducer extends Reducer<Text, Text, Text, Text> {

	MultiValueMap map = new MultiValueMap();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		List<String> users = new LinkedList<>();
		for (Text value : values)
			users.add(value.toString());
		for(int i=0;i<users.size();i++)
			for(int j=i+1;j<users.size();j++) {
				String couple = users.get(i) + "\t" + users.get(j);
				map.put(couple,key.toString());
			}	
	}

	@SuppressWarnings("unchecked")
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()) {
			String couple = it.next();
			if(map.getCollection(couple).size() >= 3)  {
				String products = this.writeProducts(map.getCollection(couple));
				context.write(new Text(couple), new Text(products));
			}
		}
	}

	private String writeProducts(Collection<String> collection) {
		String out = "";
		collection.forEach(el -> out.concat(" "+el));
		return out;
	}
}