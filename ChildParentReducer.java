package com.cc.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChildParentReducer extends Reducer<Text, Text, Text, Text>{

	private static int count = 0;
	private boolean hasParent = false;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		if(count==0){
			context.write(new Text("gr-child"),new Text("gr-parent"));
			count++;
		}
		String grchild;
		String grparent;
		Iterator<Text> iterator = values.iterator();
		//判断是否存在gr-parent
		while(iterator.hasNext()){
			//有gr-parent
			if(iterator.next().toString().startsWith("2")){
				hasParent = true;
			}
		}
	}
}
