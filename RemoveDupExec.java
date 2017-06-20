package com.cc.executor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cc.map.RemoveDupMap;
import com.cc.reduce.RemoveDupReducer;

public class RemoveDupExec {
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		
		job.setJarByClass(RemoveDupExec.class);
		
		job.setMapperClass(RemoveDupMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/hadoop/remove/data.txt"));
		
		job.setReducerClass(RemoveDupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,new Path("/hadoop/remove/result"));
		
		job.waitForCompletion(true);
		
		System.out.println("Finished");  
		
	}

}
