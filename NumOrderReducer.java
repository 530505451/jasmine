package com.cc.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumOrderReducer extends Reducer<Text, Text, IntWritable, Text>{

	private IntWritable intWritable = new IntWritable(1);
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		context.write(intWritable,key);
		intWritable.set(intWritable.get()+1);
	}
}
