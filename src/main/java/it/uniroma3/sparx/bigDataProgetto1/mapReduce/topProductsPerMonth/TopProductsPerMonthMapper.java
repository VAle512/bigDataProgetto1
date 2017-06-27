package it.uniroma3.sparx.bigDataProgetto1.mapReduce.topProductsPerMonth;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.uniroma3.sparx.bigDataProgetto1.util.TimeConverter;

public class TopProductsPerMonthMapper extends Mapper<LongWritable, Text, Text, IntWritable>	{

	private static final int PRODUCT_ID = 1;
	private static final int SCORE = 6;
	private static final int TIME = 7;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 	{

		String[] fields = value.toString().split("\t");
		
		String productId = fields[PRODUCT_ID]; 
		long time = Long.parseLong(fields[TIME]);
		int score = Integer.parseInt(fields[SCORE]);

		String monthId = TimeConverter.unix2String(time);
		String newKey = monthId + "\t" + productId;
		
		context.write(new Text(newKey), new IntWritable(score));
	}

}