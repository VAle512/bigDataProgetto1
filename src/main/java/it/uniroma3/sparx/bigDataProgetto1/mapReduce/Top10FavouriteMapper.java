package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10FavouriteMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final int PRODUCT_ID = 1;
	private static final int USER_ID = 2;
	private static final int SCORE = 6;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		
		String productID = fields[PRODUCT_ID]; 
		String userID = fields[USER_ID]; 
		int score = Integer.parseInt(fields[SCORE]);
		
		context.write(new Text(userID), new Text(productID + "\t" + score));
	}

}
