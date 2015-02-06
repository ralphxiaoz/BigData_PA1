package code.lemma;
import java.util.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import code.articles.GetArticlesMapred;
import code.articles.GetArticlesMapred.GetArticlesMapper;

import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		HashMap<String,Integer> checkword;
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here
			checkword=new HashMap<String,Integer>();
			
		
			Tokenizer tk=new Tokenizer();
			List<String> lst=new LinkedList<String>();
			lst=tk.tokenize(page.getContent());
			for(int i=0;i<lst.size();i++)
			{
				String word=lst.get(i);
				if(checkword.containsKey(word))
					checkword.put(word,checkword.get(word)+1);
				else
					checkword.put(word, 1);
			}
			StringIntegerList value=new StringIntegerList(checkword);
			Text key = new Text(page.getTitle().toString());
			context.write(key,value);
			
			
		}
	}
	public static void main(String[] args) 
	{
		Job job;
		try {
			job = new Job(new Configuration());		
			job.setJobName("Assignment1");
			job.setJarByClass(LemmaIndexMapred.class);
			job.setMapperClass(LemmaIndexMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
			
			job.setInputFormatClass(WikipediaPageInputFormat.class);
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