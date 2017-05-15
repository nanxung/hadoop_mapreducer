package com.cb.mr.friend;


import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class myPartitioner extends  HashPartitioner<myKey, DoubleWritable>{

	//执行时间越短越好
@Override
public int getPartition(myKey key, DoubleWritable value, int numReduceTasks) {
	// TODO Auto-generated method stub
	return (key.getYear()-1949)%numReduceTasks;
}
}
