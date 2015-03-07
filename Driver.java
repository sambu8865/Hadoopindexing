package hadoopprjt1;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Driver {

	
	
	
	public static void main(String[] args) throws Exception {
		// Path output = new Path("HadoopLab1");
		// File temp = new File(output.toString());
		Path output = new Path("Hadoopprjt1");
		File temp = new File(output.toString());

		if (temp.isDirectory() && temp.exists()) {
			FileUtils.deleteDirectory(temp);
			System.out.println("Deleting Folder");
		}

		Date start = new Date();
		System.out
				.println("Start........................... Program starting time: "
						+ start);

		//Configuration conf = new Configuration();
		System.out.println("Started Job");
		// JobConf job = new JobConf(prjt2.class);
		// JobClient job = new JobClient(conf, "MapReduce 1");

		JobConf job = new JobConf(Driver.class);
		// job.setJobName("Driver");

		job.setJarByClass(Driver.class);
		job.setMapperClass(MapperOne.class);
		job.setReducerClass(ReducerOne.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		/*
		 * job.waitForCompletion(true);
		 * 
		 * Job job2 = new Job(conf, "MapReduce 2");
		 * job2.setJarByClass(prjt1.class);
		 * 
		 * job2.setMapperClass(prjt1.Mappertwo.class);
		 * job2.setReducerClass(prjt1.Reducertwo.class);
		 * 
		 * job2.setOutputKeyClass(Text.class);
		 * job2.setOutputValueClass(IntWritable.class);
		 * 
		 * job2.setMapOutputKeyClass(Text.class);
		 * job2.setMapOutputValueClass(Text.class);
		 * job2.setOutputKeyClass(Text.class);
		 * job2.setOutputValueClass(NullWritable.class);
		 * 
		 * org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job
		 * , new Path(args[1]));
		 * org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
		 * .setOutputPath(job, new Path(args[2]));
		 * 
		 * job2.waitForCompletion(true);
		 */
		// job.waitForCompletion(true);

		Date finish = new Date();
		
		System.out.println("Program ending time  " + finish);
		Long diff = (finish.getTime() - start.getTime());
		System.out.println("Driver Programs start time is: " + start
				+ " , and end time is: " + finish);
		System.out.println("Total time of execution: " + diff
				+ " milliseconds.");
		
		
	}

}
