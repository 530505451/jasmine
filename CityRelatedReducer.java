package com.cc.reduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CityRelatedReducer extends Reducer<Text, Text, Text, Text>{

	private static int count = 0 ;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		if(count==0){
			context.write(new Text("username"),new Text("cityname"));
			count++;
		}
		String username = null;
		String cityname = null;
		Iterator<Text> iterator = values.iterator();
		while(iterator.hasNext()){
			String userOrCity = iterator.next().toString();
			if(userOrCity.startsWith("1")){
				username = userOrCity;
			}
			else{
				cityname = userOrCity;
			}
		}
		context.write(new Text(username),new Text(cityname));
	}
}
