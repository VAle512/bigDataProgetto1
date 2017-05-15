package it.uniroma3.sparx.bigDataProgetto1.mapReduce.relatedUsers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelatedUsers {

	public static void main(String[] args) {

		Job job = null;
		try {
			job = new Job(new Configuration(), RelatedUsers.class.getSimpleName());
		} catch (IOException e) {
			System.out.println("[ERR] Error while creating new Job");
			e.printStackTrace();
			System.exit(1);
		}

		job.setJarByClass(RelatedUsers.class);
		job.setMapperClass(RelatedUsersMapper.class);
		job.setReducerClass(RelatedUsersReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RelatedUsersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RelatedUsersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RelatedUsersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, RelatedUsersMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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
