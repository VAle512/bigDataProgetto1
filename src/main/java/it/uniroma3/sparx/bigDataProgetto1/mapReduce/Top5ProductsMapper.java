package it.uniroma3.sparx.bigDataProgetto1.mapReduce;


import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top5ProductsMapper extends Mapper<LongWritable, Text, Text, IntWritable>	{

	private static final int PRODUCT_ID = 1;
	private static final int SCORE = 6;
	private static final int TIME = 7;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 	{

		String[] fields = value.toString().split("\t");
		
		int productId = Integer.parseInt(fields[PRODUCT_ID]); 
		long time = Long.parseLong(fields[TIME]);
		int score = Integer.parseInt(fields[SCORE]);

		Calendar date = Calendar.getInstance();
		date.setTimeInMillis(time*1000);
		
		String newKey = "";
		newKey += date.get(Calendar.YEAR);
		if(date.get(Calendar.MONTH) < 9)
			newKey += "0";
		newKey += (date.get(Calendar.MONTH) +1 );
		newKey += " ";
		newKey += productId;
		
		context.write(new Text(newKey), new IntWritable(score));
	}

}