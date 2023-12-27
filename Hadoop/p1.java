import java.io.IOException;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import java.net.URI;

public class p1 {
	public static class HW01Mapper
	extends Mapper<Object, Text, Text, Text>{
		
		private Text hadoopKey = new Text();
		private Text hadoopValue = new Text();
      
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
					List<Integer> keyList = new ArrayList<Integer>();
										
					String input[] = value.toString().split("\\t"); // input line 0	1,2,3
					
					keyList.add(Integer.parseInt(input[0]));
					int keyOne = Integer.parseInt(input[0]);
					
					if( input.length == 2) {
						// if there aren't at least 2 values, then we have a user that has no friends
						String keyLine = input[0]; // 0
						String keyValue = input[1]; // 1,2,3
						
						String keyValues[] = keyValue.split(",");
						
						for ( String lineValue : keyValues){
							int keyTwo = Integer.parseInt(lineValue);
							String keyString;
							if(keyOne < keyTwo) {
								keyString = keyLine + "," + lineValue;
							}else {
								keyString = lineValue + "," + keyLine;
							}
	
							hadoopKey.set(keyString);
							hadoopValue.set(keyValue);
							context.write(hadoopKey, hadoopValue);
						}
					}
			}
	}
  
	public static class HW01Reducer 
    extends Reducer<Text,Text,Text,Text> {
	  
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context
               ) throws IOException, InterruptedException {
					List<String> arr1 = new ArrayList<String>();
					List<String> arr2= new ArrayList<String>();
					
					for (Text val : values) {      // (f1,f2 , (1,2,3))
						// first pass arr1 will be empty
						if(arr1.size() <= 0) {
							arr1 = Arrays.asList(val.toString().split(","));
						}else {
							arr2 = Arrays.asList(val.toString().split(","));
						}
					}
					StringBuilder sBuild = new StringBuilder();
					for( String s : arr1) {
						if(arr2.contains(s)) {
							sBuild.append(s + ",");
						}
					}
					if( sBuild.length() > 0) {
						sBuild.deleteCharAt(sBuild.length() - 1);
						result.set(sBuild.toString());
					}else {
						// there were no mutual friends
						result.set("");
					}
					context.write(key,  result);
			}
	}
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    // Part 1, calculate mutual friends
		    Job job = new Job(conf, "HW01 Part1");
		    job.setJarByClass(p1.class);
		    job.setMapperClass(HW01Mapper.class);
		    job.setReducerClass(HW01Reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    long startTime = System.currentTimeMillis();
		    if (!job.waitForCompletion(true)) {
		        System.exit(1);
		    }
		    long endTime = System.currentTimeMillis();
	        long executionTime = endTime - startTime;
	        System.out.println("Runtime in ms:"+executionTime);	
  }
}
