package com.cb.sort;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReducer extends Reducer<Text,IntWritable,NullWritable,Text> {

	protected void reduce(Text arg0, Iterable<Text> arg1,
			Reducer<Text, IntWritable, NullWritable, Text>.Context arg2) throws IOException, InterruptedException {
		for(Text value:arg1){
			arg2.write(NullWritable.get(),value);
		}
	}


	

}
