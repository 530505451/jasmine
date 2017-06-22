package com.cc.executor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cc.map.CityRelated1Mapper;
import com.cc.map.CityRelated2Mapper;
import com.cc.reduce.CityRelatedReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class CityRelatedExec {
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		
		job.setJarByClass(CityRelatedExec.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(CityRelatedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path("/hadoop/city/data1.txt"), FileInputFormat.class, CityRelated1Mapper.class);
		MultipleInputs.addInputPath(job, new Path("/hadoop/city/data2.txt"), FileInputFormat.class, CityRelated2Mapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/hadoop/city/result"));
		
		job.waitForCompletion(true);
		System.out.println("finished");
	}

}
