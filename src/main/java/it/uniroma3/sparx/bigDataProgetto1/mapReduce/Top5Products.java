package it.uniroma3.sparx.bigDataProgetto1.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class Top5Products {
	
	public static void main(String[] args) throws IOException {

//        JobConf conf = new JobConf(Top5Products.class);
//        conf.setJobName("Best 5 Product");
//        
//        FileInputFormat.addInputPath(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//        
//        conf.setMapperClass(Best5ProductMapper.class);
//        conf.setReducerClass(Top5ProductsReducer.class);
//        
//        conf.setMapOutputKeyClass(Text.class);
//        conf.setMapOutputValueClass(IntWritable.class);
//        
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(DoubleWritable.class);
//        
//        JobClient.runJob(conf);
        
    }

}