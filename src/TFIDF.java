/*
* CSC548 - HW4 Question 1- Calculate TFIDF using the hadoop on set of document
*  Author: ajain28 Abhash Jain
*
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
			System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WordCount");
		
		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		
		/************ YOUR CODE HERE ************/
		
		// Create and execute Word Count job
		System.out.println("Working with task");
		Job job = Job.getInstance(conf,"word count");
		job.setJarByClass(TFIDF.class);
		//Set wordcount Mapper and reducer class
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		//Set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,wcInputPath);
		FileOutputFormat.setOutputPath(job,wcOutputPath);
		job.waitForCompletion(true);
		System.out.println("Task Completed!");
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Split each word and mark its count to 1.
		System.out.println("Entered map function");
		 String docText = value.toString().replaceAll("[^a-zA-Z ]", "");
		//if(!title.matches("[a-zA-Z0-9_\s]+")) {
  	    //lemmatization is remaining
  	    // https://stackoverflow.com/questions/1578062/lemmatization-java
  	    
		String[] words = docText.split(" ");
		String doc_name = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		for (String word : words){
        	context.write(new Text(doc_name), new Text(word));
		}
	}
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, Text, Text, Text> {
	
		Set<String> dictonary = new HashSet<String>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("Setup function");
			Path pt = new Path("hdfs://node-master:9000/user/ajain28/dictonary/d.txt");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			while (line!=null) {
				dictonary.add(line.toLowerCase());
				line = br.readLine();
            }
		}

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
		System.out.println( "Dictonary size " + dictonary.size());
		System.out.println("Reducer called");
		for(Text t : values){
			System.out.println("t value is: " + t.toString());
			if(!dictonary.contains(t.toString().toLowerCase()))
				context.write(key,t);
		}
    }
}
}