package com.cc.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RemoveDupReducer extends Reducer<Text, Text, Text, Text>{

	private Text val = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		for (Text i : values) {
			val.set(i);
		}
		context.write(new Text(key),val);
	}

	
	

}
