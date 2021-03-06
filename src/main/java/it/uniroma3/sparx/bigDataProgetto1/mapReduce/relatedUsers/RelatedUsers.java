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

		//adding timestamp to the path prevent failures if old tmp folder still exists.
		String tmpFolderPath =  "tmp_"+ Long.toString(System.currentTimeMillis());
		//temporary output path for 1st reducer
		Path tmpPath = new Path(tmpFolderPath);

		Job job1 = null;
		Job job2 = null;
		try {
			job1 = new Job(new Configuration(), RelatedUsers.class.getSimpleName()+"1");
			job2 = new Job(new Configuration(), RelatedUsers.class.getSimpleName()+"2");
		} catch (IOException e) {
			System.out.println("[ERR] Error while creating new Job");
			e.printStackTrace();
			System.exit(1);
		}	

		job1.setJarByClass(RelatedUsers.class);
		job1.setMapperClass(RelatedUsersMapper1.class);
		job1.setReducerClass(RelatedUsersReducer1.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RelatedUsersMapper1.class);
		FileOutputFormat.setOutputPath(job1, tmpPath);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job2.setJarByClass(RelatedUsers.class);
		job2.setMapperClass(RelatedUsersMapper2.class);
		job2.setReducerClass(RelatedUsersReducer2.class);
		MultipleInputs.addInputPath(job2, tmpPath, TextInputFormat.class, RelatedUsersMapper2.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		try {
			long start = System.currentTimeMillis();
			job1.waitForCompletion(true);
			job2.waitForCompletion(true);
			long elapsed = System.currentTimeMillis() - start;
			System.out.println("TEMPO TRASCORSO = "+elapsed/1000.0+" secondi.");
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			System.out.println("[ERR] Error while executing Map Reduce");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
