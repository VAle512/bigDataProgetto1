package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5Products {

	public static void main(String[] args) {

		Job job = null;
		try {
			job = new Job(new Configuration(), "Top5Products");
		} catch (IOException e) {
			System.out.println("[ERR] Error while creating new Job");
			e.printStackTrace();
			System.exit(1);
		}

		job.setJarByClass(Top5Products.class);
		job.setMapperClass(Top5ProductsMapper.class);
		job.setReducerClass(Top5ProductsReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Top5ProductsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Top5ProductsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Top5ProductsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, Top5ProductsMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			System.out.println("[ERR] Error while executing Map Reduce");
			e.printStackTrace();
			System.exit(1);
		}
	}
}