package com.cc.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChildParentMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String childName;
		String parentName;
		String[] values = value.toString().split(" ");
		//不是行头
		if(values[0].compareTo("child")!=0){
			childName = values[1];
			parentName = values[0];
			//左表
			context.write(new Text(parentName),new Text("1"+childName));
			//右表
			context.write(new Text(childName),new Text("2"+parentName));
		}
	}
}
