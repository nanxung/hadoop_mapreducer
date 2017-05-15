package com.cb.mr.weather;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class RunJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://192.168.1.199:8020");
		config.set("yarn.resourcemanager.hostname", "SingleNode");
		// config.set("mapred.jar",
		// "C:\\Users\\Administrator\\Desktop\\wc.jar");
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);

			job.setJobName("weather");
			job.setMapperClass(WeatherMapper.class);
			job.setReducerClass(WeatherReduce.class);
			job.setMapOutputKeyClass(myKey.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			job.setPartitionerClass(myPartitioner.class);
			job.setSortComparatorClass(mySort.class);
			job.setGroupingComparatorClass(myGroup.class);
			
			job.setNumReduceTasks(3);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path("/data/input/weather"));

			Path outpath = new Path("/data/out5");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("jobï¿½ï¿½ï¿½ï¿½Ö´ï¿½Ð³É¹ï¿½");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// keyÃ¿Ò»ÐÐµÚÒ»¸ö¸ô¿ª·û×ó±ßÎªkey£¬ÓÒ±ßÎªvalue
	static class WeatherMapper extends Mapper<Text, Text, myKey, DoubleWritable> {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		NullWritable v = NullWritable.get();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, myKey, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				java.util.Date date = sdf.parse(key.toString());
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH);
				double hot = Double.parseDouble(value.toString().substring(0, value.toString().lastIndexOf("c")));
				myKey k = new myKey();
				k.setYear(year);
				k.setHot(hot);
				k.setMonth(month);
				context.write(k, new DoubleWritable(hot));
			} catch (Exception e) {

			}
		}
	}
	
	static class WeatherReduce extends Reducer<myKey, DoubleWritable, Text, NullWritable>{
		
		@Override
		protected void reduce(myKey arg0, Iterable<DoubleWritable> arg1,
				Reducer<myKey, DoubleWritable, Text, NullWritable>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int i=0;
			for(DoubleWritable v:arg1){
				i++;
				String msg=arg0.getYear()+"\t"+arg0.getMonth()+"\t"+v.get();
				arg2.write(new Text(msg), NullWritable.get());
				if(i==3){
					break;
				}
			}
		}
	}
	
	
	
	
	
	
	
	
	
	

}
