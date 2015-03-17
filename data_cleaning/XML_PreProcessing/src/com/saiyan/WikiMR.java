package com.saiyan;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class WikiMR {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, NullWritable, Text>{

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String xmlstring = value.toString();
			try { 
			SAXBuilder builder = new SAXBuilder();
			Reader in = new StringReader(xmlstring);
			System.out.println("****************************************************");
			System.out.println("Element: "+key);
			Document doc = builder.build(in);
				Element root = doc.getRootElement();
				String title = root.getChild("title").getTextTrim();
				String text = root.getChild("text").getTextTrim();
				if (title.toLowerCase().contains("film") == true)
				{
					context.write(NullWritable.get(), new Text(title.toLowerCase()+ "\001"+ text.toLowerCase()));
				}


			} catch (JDOMException jdomexception)
			{
				Logger.getLogger(WikiMR.class.getName()).log(Level.SEVERE,null,jdomexception);
				System.out.println(xmlstring);
			}
			catch (IOException ioexception)
			{
				Logger.getLogger(WikiMR.class.getName()).log(Level.SEVERE,null,ioexception);
			}
			catch (Exception exception)
			{
				Logger.getLogger(WikiMR.class.getName()).log(Level.SEVERE,null,exception);
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("start.tag","<DOC>");
		conf.set("end.tag","</DOC>");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "WikiMR");
		job.setJarByClass(WikiMR.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(XMLoader.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}