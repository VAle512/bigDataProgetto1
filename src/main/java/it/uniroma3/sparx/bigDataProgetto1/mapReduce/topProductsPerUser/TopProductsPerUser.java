package it.uniroma3.sparx.bigDataProgetto1.mapReduce.topProductsPerUser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopProductsPerUser {

	public static void main(String[] args) {

		Job job = null;
		try {
			job = new Job(new Configuration(), TopProductsPerUser.class.getSimpleName());
		} catch (IOException e) {
			System.out.println("[ERR] Error while creating new Job");
			e.printStackTrace();
			System.exit(1);
		}

		job.setJarByClass(TopProductsPerUser.class);
		job.setMapperClass(TopProductsPerUserMapper.class);
		job.setReducerClass(TopProductsPerUserReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TopProductsPerUserMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		try {
			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long elapsed = System.currentTimeMillis() - start;
			System.out.println("TEMPO TRASCORSO = "+elapsed/1000.0+" secondi.");
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			System.out.println("[ERR] Error while executing Map Reduce");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
