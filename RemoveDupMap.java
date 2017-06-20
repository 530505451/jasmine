package com.cc.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RemoveDupMap extends Mapper<LongWritable,Text,Text,Text>{

	private String time;
	private String val;
	private Map<String,String> map = new HashMap<>();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while(tokenizer.hasMoreTokens()){
			time = tokenizer.nextToken();
			val = tokenizer.nextToken();
		}
		if(!map.containsKey(time)){
			map.put(time, val);
		}
		context.write(new Text(time),new Text(val));
	}
}
