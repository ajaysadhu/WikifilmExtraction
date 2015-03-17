package com.saiyan;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ClenseXML {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, NullWritable, Text>{

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String xmlstring = value.toString();
			String word="";
			String outline = " ";
			StringTokenizer tokenizer = new StringTokenizer(xmlstring);
			while (tokenizer.hasMoreTokens()) {
				word="";
				word = tokenizer.nextToken();


				if (word.contains("<") == true && word.contains(">") == true)
				{

				}	
				else if (word.contains(">") == true) {

					word = word.replace(">", "gt;");

				}
				else if (word.contains("<") == true) {
					word = word.replace("<", "lt;");

				}
				outline = outline + " "+ word;
			}

			try {


				if (outline.endsWith("<docno>"))	
				{

				}

				else 
				{
					context.write(NullWritable.get(), new Text(outline.replace("&","")));
				}
			}catch (IOException ioexception)
			{
				Logger.getLogger(WikiMR.class.getName()).log(Level.SEVERE,null,ioexception);
			}


		}

	}
	static
	{
		long data=0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		
		
		Job job = new Job(conf, "ClenseXML");
		job.setJarByClass(ClenseXML.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}