package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5Products {

	public static void main(String[] args) throws Exception{

		Job job = new Job(new Configuration(), "Top5Products");

		job.setJarByClass(Top5Products.class);
		job.setMapperClass(Top5ProductsMapper.class);
		job.setReducerClass(Top5ProductsReducer.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Top5ProductsMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Top5ProductsMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Top5ProductsMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, Top5ProductsMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

	}

}