
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class mutualFriend {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                StringTokenizer it = new StringTokenizer(value.toString(), "\n");
                String line = null;
                String[] lineArray = null;
                String[] friendArray = null;
                String[] tempArray = null;
                while(it.hasMoreTokens()){
                        line = it.nextToken();
                        lineArray = line.split("\t");
                        if(lineArray.length > 1) {
	                        friendArray = lineArray[1].split(",");
	                        tempArray = new String[2];
	                        for(int i = 0; i < friendArray.length; i++){
	                        		if( Integer.parseInt(friendArray[i]) >= Integer.parseInt(lineArray[0]) ) {
	                        			tempArray[1] = friendArray[i];
		                                tempArray[0] = lineArray[0];
	                        		}
	                        		else {
	                        			tempArray[0] = friendArray[i];
		                                tempArray[1] = lineArray[0];
	                        		}
	                                
	                                context.write(new Text(tempArray[0] + "," + tempArray[1]+"\t"), new Text(lineArray[1]));
	                        }
                        }    
                }
        }
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> input = new ArrayList<>();
			input.add("0,4	");
			input.add("20,22939	");
			input.add("1,29826	");
			input.add("6222,19272	");
			input.add("28041,28056	");
			Text []ar = new Text[2];
			int index = 0;
			for(Text it:values)
				ar[index++] = new Text(it);
			String []list1 = ar[0].toString().split(",");
			String []list2 = ar[1].toString().split(",");
			String res = "";
			int k = 0;
			for(int i = 0; i < list1.length; i++) {
				for(int j = 0; j < list2.length; j++) {
					if(list1[i].equals(list2[j]) && k == 0) {
						
						res = res.concat(list1[i]);
						k++;
						break;
					}
					if(list1[i].equals(list2[j]) && k > 0) {
						
						res = res.concat(","+list1[i]);
						k++;
						break;
					}
				}
			}
			if(input.contains(key.toString()))
				context.write( key, new Text(res) );
				
		}
		
	}
	
	public static void main(String []args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
		System.err.println("Usage: WordCount <in> <out>");
		System.exit(2);
		}
		Job job = new Job(conf, "mutualFriend");
		
		job.setJarByClass(mutualFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);	

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}
}
