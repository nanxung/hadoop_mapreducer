package friend;

import java.io.IOException;



import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import com.cb.mr.weather.myKey;


public class RunJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://yun0:8020");
		config.set("yarn.resourcemanager.hostname", "yun0");
		// config.set("mapred.jar",
		// "C:\\Users\\Administrator\\Desktop\\wc.jar");
		run1(config);
	}
	
	public static void run1(Configuration config){
		
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);

			job.setJobName("frend");
			job.setMapperClass(FofMapper.class);
			job.setReducerClass(FofReduce.class);
			job.setMapOutputKeyClass(Fof.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setNumReduceTasks(3);
			
			FileInputFormat.addInputPath(job, new Path("/data/input/friend"));

			Path outpath = new Path("/data/out6");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job����ִ�гɹ�");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class FofMapper extends Mapper<Text, Text, Fof, IntWritable> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Fof, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String user=key.toString();
			String[] friends=StringUtils.split(value.toString(),'\t');
			for(int i=0;i<friends.length;i++){
				String f1=friends[i];
				Fof ofof=new Fof(user,f1);
				context.write(ofof, new IntWritable(0));
				for(int j=i+1;j<friends.length;j++){
					String f2=friends[j];
					Fof fof=new Fof(f1,f2);
					context.write(fof, new IntWritable(1));
				}
			}
		}
	}
	

	static class FofReduce extends Reducer<Fof, IntWritable, Fof, IntWritable>
	{
		@Override
		protected void reduce(Fof arg0, Iterable<IntWritable> arg1,
				Reducer<Fof, IntWritable, Fof, IntWritable>.Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			boolean f=true;
			for(IntWritable i:arg1){
				if(i.get()==0){
					f=false;
					break;
				}else{
					sum=sum+i.get();
				}
			}
			if(f){
				arg2.write(arg0, new IntWritable(sum));
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
