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

public class p2p3 {
	public static class HW01Mapper4
	extends Mapper<Object, Text, Text, Text>{
		
		private Text hadoopKey = new Text();
		private Text hadoopValue = new Text();
		public static int maxFrnds = 0;
      
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
										
					String input[] = value.toString().split("\\t"); // input line '0,1	1,2,3'
					
					// get the 2nd value
					if( input.length == 2) {
						// if there aren't at least 2 values, then we have a user that has no friends
						String keyValue = input[1]; // 1,2,3						
						String keyValues[] = keyValue.split(",");
						int Frndscount = keyValues.length;
						if(Frndscount > maxFrnds) {
							maxFrnds = Frndscount;
						}
						hadoopKey.set(input[0]);
						hadoopValue.set(Integer.toString(keyValues.length));
						context.write(hadoopKey,  hadoopValue);
					}
			}
	}
  
	public static class HW01Reducer4 
    extends Reducer<Text,Text,Text,Text> {
	  
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context
               ) throws IOException, InterruptedException {
			HW01Mapper4 obj = new HW01Mapper4();
			int maxcount = obj.maxFrnds;
					for(Text val:values) {
						if(Integer.parseInt(val.toString())== maxcount) {
							result.set(Integer.toString(maxcount));
							context.write(key, result);
						}
					}	
			}
	}
	
	public static class HW01Mapper5
	extends Mapper<Object, Text, Text, Text>{
		
		private Text hadoopKey = new Text();
		private Text hadoopValue = new Text();
      
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
										
					String input[] = value.toString().split("\\t"); // input line '0,1	1,2,3'
					
					// get the 2nd value
					if( input.length == 2) {
						// if there aren't at least 2 values, then we have a user that has no friends
						String keyValue = input[1]; // 1,2,3						
						hadoopKey.set(input[0]);
						hadoopValue.set(keyValue);
						context.write(hadoopKey,  hadoopValue);
					}
			}
	}
  
	public static class HW01Reducer5 
    extends Reducer<Text,Text,Text,Text> {
	  
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context
               ) throws IOException, InterruptedException {
				 int count = 0;
				 for(Text val : values) {
					 String keyValues[] = val.toString().split(",");
					 for(String value:keyValues) {
							if(val.charAt(0) == '1' || val.charAt(0) == '5') {
								count++;
							}
						}
					 if(count > 0) {
							List<String> specificVals = new ArrayList<String>();
							for(String value:keyValues) {
								if(value.charAt(0) == '1' || value.charAt(0) == '5') {
									specificVals.add(value);
								}
							}
							StringBuilder temp = new StringBuilder();
							for(String s : specificVals) {
								temp.append(s+",");
							}
							result.set(temp.toString());
							context.write(key,result);
						}
				 }
			}
	}

	public static class HW01Mapper2
	extends Mapper<Object, Text, Text, Text>{
		
		private Text hadoopKey = new Text();
		private Text hadoopValue = new Text();
      
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
										
					String input[] = value.toString().split("\\t"); // input line '0,1	1,2,3'
					
					// get the 2nd value
					if( input.length == 2) {
						// if there aren't at least 2 values, then we have a user that has no friends
						String keyValue = input[1]; // 1,2,3						
						String keyValues[] = keyValue.split(",");
						
						hadoopKey.set("AVERAGE");
						hadoopValue.set(Integer.toString(keyValues.length));
						context.write(hadoopKey,  hadoopValue);
					}
			}
	}
  
	public static class HW01Reducer2 
    extends Reducer<Text,Text,Text,Text> {
	  
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context
               ) throws IOException, InterruptedException {
			
				    int sum = 0;
				    int count = 0;
				    for (Text val : values) {
				    	sum += Integer.parseInt(val.toString());
				    	count ++;
				      }
				    
				      result.set(Double.toString(((double)sum)/((double)count)));
				      
				      context.write(key, result);
				      
			}
	}
	
	public static class HW01Mapper3
	extends Mapper<Object, Text, Text, Text>{
		
		private Text hadoopKey = new Text();
		private Text hadoopValue = new Text();
      
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
										
					String input[] = value.toString().split("\\t"); // input line '0,1	1,2,3'
					
					String avgString = context.getConfiguration().get("average");
					double avg = Double.parseDouble(avgString);

					// get the 2nd value
					if( input.length == 2) {
						// if there aren't at least 2 values, then we have a user that has no friends
						String keyValue = input[1]; // 1,2,3						
						String keyValues[] = keyValue.split(",");
						
						hadoopKey.set(input[0]);
						if(keyValues.length >= avg) {
							hadoopValue.set("[ "+input[1]+" ]");
							context.write(hadoopKey,  hadoopValue);
						}
					}
			}
	}
  
	public static class HW01Reducer3 
    extends Reducer<Text,Text,Text,Text> {
	  
		private Text result = new Text();
		public void reduce(Text key, Text values, Context context
               ) throws IOException, InterruptedException {
			
				  // there is nothing to map, which likely means i did this wrong
			      context.write(key, values);
				      
			}
	}
	
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    conf = new Configuration();
    
    // Part 2, finding pair of frnds with highest mutual frnds
    Job job5 = new Job(conf, "HW01 Part2");
    job5.setJarByClass(p2p3.class);
    job5.setMapperClass(HW01Mapper4.class);
    job5.setReducerClass(HW01Reducer4.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job5, new Path(args[0]));
    FileOutputFormat.setOutputPath(job5, new Path(args[1]));
    long startTime = System.currentTimeMillis();
    if (!job5.waitForCompletion(true)) {
        System.exit(1);
    }
    
    conf = new Configuration();
    // Part 2, displaying the mutual friends whos initial starts with 1 or 5
    Job job6 = new Job(conf, "HW01 Part2-02");
    job6.setJarByClass(p2p3.class);
    job6.setMapperClass(HW01Mapper5.class);
    job6.setReducerClass(HW01Reducer5.class);
    job6.setOutputKeyClass(Text.class);
    job6.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job6, new Path(args[0]));
    FileOutputFormat.setOutputPath(job6, new Path(args[2]));
    if (!job6.waitForCompletion(true)) {
        System.exit(1);
    }
    long endTime = System.currentTimeMillis();
    long executionTime1 = endTime - startTime;	
    
    // Part 3, calculate average number of mutual friends and provide list of friends 
    // 		with mutual friends above the average
    conf = new Configuration();
    Job job3 = new Job(conf, "HW01 Part3");
    job3.setJarByClass(p2p3.class);
    job3.setMapperClass(HW01Mapper2.class);
    job3.setReducerClass(HW01Reducer2.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    job3.setNumReduceTasks(1); // limit to a single reducer since avg will only have one key
    FileInputFormat.addInputPath(job3, new Path(args[0])); // take the output from the first job as input
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    long startTime1 = System.currentTimeMillis();
    if (!job3.waitForCompletion(true)) {
        System.exit(1);
    }
    
    String uri = "hdfs://localhost:9000/user/ap43n/"+args[3]+"/part-r-00000";
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	InputStream in = null;
	OutputStream os = new ByteArrayOutputStream();
	try {
		in = fs.open(new Path(uri));
		IOUtils.copyBytes(in, os, 4096, false);
	} finally {
		IOUtils.closeStream(in);
	}
	String avgString = os.toString();
	String avgvalues[] = avgString.split("\\t");

    
    // Part 3, calculate average number of mutual friends and provide list of friends 
    // 		with mutual friends above the average
    conf = new Configuration();
	conf.set("average",avgvalues[1]);
    Job job4 = new Job(conf, "HW01 Part3a");
    job4.setJarByClass(p2p3.class);
    job4.setMapperClass(HW01Mapper3.class);
    job4.setReducerClass(HW01Reducer3.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    job4.setNumReduceTasks(1); // limit to a single reducer since avg will only have one key
    FileInputFormat.addInputPath(job4, new Path(args[0])); // take the output from the first job as input
    FileOutputFormat.setOutputPath(job4, new Path(args[4]));
    if (!job4.waitForCompletion(true)) {
        System.exit(1);
    }
    long endTime1 = System.currentTimeMillis();
    long executionTime2 = endTime1 - startTime1;

    System.out.println("Runtime in ms:"+executionTime1);
    System.out.println("Runtime in ms:"+executionTime2);	
    
  }
}
