package it.uniroma3.sparx.bigDataProgetto1.mapReduce.topProductsPerMonth;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopProductsPerMonth {

	public static void main(String[] args) {

		Job job = null;
		try {
			job = new Job(new Configuration(), TopProductsPerMonth.class.getSimpleName());
		} catch (IOException e) {
			System.out.println("[ERR] Error while creating new Job");
			e.printStackTrace();
			System.exit(1);
		}

		job.setJarByClass(TopProductsPerMonth.class);
		job.setMapperClass(TopProductsPerMonthMapper.class);
		job.setReducerClass(TopProductsPerMonthReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TopProductsPerMonthMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		try {
			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long elapsed = System.currentTimeMillis() - start;
			System.out.println("TEMPO TRASCORSO = "+elapsed/10000+" secondi.");
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			System.out.println("[ERR] Error while executing Map Reduce");
			e.printStackTrace();
			System.exit(1);
		}
	}
}