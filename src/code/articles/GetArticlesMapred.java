package code.articles;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;


/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
@SuppressWarnings("deprecation")
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage , Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>(); //people's name matches articletitle

		protected void setup(Mapper<LongWritable,WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			
			URI[] localPath =context.getCacheFiles();
			File f =new File(localPath[0].toString());
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line= new String("");
			while((line=br.readLine())!= null)
			{
				peopleArticlesTitles.add(line); //load name into set
			}
			br.close();
			//super.setup(context);
		}
		
		 protected static enum MyCounter {
		      INPUT_WORDS, INPUT_ARTICLES, SAMPLED_ARTICLES
		    };
		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here

			//String content=null;

			if(peopleArticlesTitles.contains(inputPage.getTitle()))
			{
				//content= new String(inputPage.getContent());
				Text key =new Text(offset.toString());
				Text value =new Text(inputPage.getRawXML().toString());
				context.write(key,value);
			}
		}
	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		//new job
		Job job;
		try {
			job = new Job(new Configuration());		
			job.setJobName("Assignment1");
			job.setJarByClass(GetArticlesMapred.class);
			job.setMapperClass(GetArticlesMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(WikipediaPageInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//job.addCacheFile(new Path("hdfs://ubuntu:9000/user/ljf/people.txt").toUri());
			try {
				DistributedCache.addCacheFile(new URI("/home/ralph/workspace/BigData_PA1_WikiPage/people.txt"), job.getConfiguration());
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}