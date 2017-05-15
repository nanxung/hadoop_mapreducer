package com.cb.mr.friend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class RunJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://192.168.1.199:9000");
		config.set("yarn.resourcemanager.hostname", "SingleNode");
		// config.set("mapred.jar",
		// "C:\\Users\\Administrator\\Desktop\\wc.jar");
		run1(config);
	}
	
	public static void run1(Configuration config){
		
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);

			job.setJobName("weather");
			job.setNumReduceTasks(3);
			
			FileInputFormat.addInputPath(job, new Path("/data/input/friend"));

			Path outpath = new Path("/data/out5");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println(" Íê³É");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class FofMapper extends Mapper<Text, Text, Fof, IntWritable> {
		
	}
	

}
