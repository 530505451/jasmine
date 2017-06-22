package com.cc.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CityRelated2Mapper extends Mapper<LongWritable,Text,Text,Text>{

	private static int count = 0;
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		if(count==0){
			count++;
			return;
		}
		String[] values = value.toString().split(" ");
		context.write(new Text(values[0]),new Text("2"+values[1]));
	}

}
