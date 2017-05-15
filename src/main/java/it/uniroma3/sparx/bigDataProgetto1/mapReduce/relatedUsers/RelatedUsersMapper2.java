package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelatedUsersMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tk = new StringTokenizer(value.toString(), "\t");
		String couple = tk.nextToken() +"\t"+tk.nextToken();
		String producID = tk.nextToken();
		context.write(new Text(couple), new Text(producID));
	}

}
