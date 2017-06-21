package com.cc.executor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cc.map.NumOrderMapper;
import com.cc.reduce.NumOrderReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class NumOrderExec {
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		
		job.setJarByClass(NumOrderExec.class);
		
		job.setMapperClass(NumOrderMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/hadoop/order/data.txt"));
		
		job.setReducerClass(NumOrderReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/hadoop/order/result"));
		
		job.waitForCompletion(true);
		System.out.println("finished");
	}

}
