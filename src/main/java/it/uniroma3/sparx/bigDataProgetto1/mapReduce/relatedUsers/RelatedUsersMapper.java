package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelatedUsersMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final int PRODUCT_ID = 1;
	private static final int USER_ID = 2;
	private static final int SCORE = 6;
	private static final int MINIMUM_SCORE = 4;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");

		String productId = fields[PRODUCT_ID];
		String userId = fields[USER_ID];
		int score = Integer.parseInt(fields[SCORE]);

		if(score >= MINIMUM_SCORE)
			context.write(new Text(productId), new Text(userId));
	}

}
