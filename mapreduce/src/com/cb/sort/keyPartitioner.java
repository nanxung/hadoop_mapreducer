package com.cb.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class keyPartitioner extends HashPartitioner<Text,NullWritable> {

	@Override
	public int getPartition(Text key, NullWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (key.toString().split(" ")[0].hashCode() & Integer.MAX_VALUE)%numReduceTasks;
	}
	

}
