package code.inverted;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import code.lemma.LemmaIndexMapred;
import code.lemma.LemmaIndexMapred.LemmaIndexMapper;

import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, StringInteger> {

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
			Set dict=new HashSet<String>();
			String indices_str=indices.toString();
			int key_index=indices_str.indexOf('<');
			String key=indices_str.substring(0, key_index);//get the key and clean it from indices
			indices_str=indices_str.replace(key,"");
		
			while(indices_str.length()!=0)//keep reading tuples and clean it from indices_str
			{
				int value_index_begin=indices_str.indexOf('<')+1;
				int value_index_end=indices_str.indexOf('>');
				String tuple=indices_str.substring(value_index_begin, value_index_end);
				int corner=tuple.indexOf(',');
				String value=tuple.substring(0,corner);
				String time=tuple.substring(corner+1);
				indices_str=indices_str.replace("<"+tuple+">","");
				indices_str=indices_str.replaceFirst(",", "");
				
				StringInteger output_value= new StringInteger(key,Integer.parseInt(time));
				Text output_key=new Text(value);
				context.write(output_key, output_value);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> dict = new HashMap<String,Integer>();
			for(StringInteger it: articlesAndFreqs)
			{
				String key=it.getString();
				int value=it.getValue();
				dict.put(key, value);
			}
			StringIntegerList stl=new StringIntegerList(dict);
			context.write(lemma, stl);
			// TODO: You should implement inverted index reducer here
		}
	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Job job;
		try {
			job = new Job(new Configuration());		
			job.setJobName("Assignment1_part3");
			job.setJarByClass(InvertedIndexMapred.class);
			job.setMapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(StringInteger.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
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