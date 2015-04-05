package edu.pitt.sis.infsci2711.lettercount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.regex.Pattern;
 
// tell hadoop what it needs to know to run your program in a configuration object
// use ToolRunner to run your MapReduce application
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
// send debugging message from inside the mapper and reducer classes
import org.apache.log4j.Logger;
 
// Job class in order to create, configure, and run an instance of your MapReduce application
import org.apache.hadoop.mapreduce.Job;
 
// extend the Mapper class and Reducer class to our own Map and Reducer class
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 
// use the Path class to access files in HDFS
import org.apache.hadoop.fs.Path;
 
// in job configuration instructions, pass required paths
// using the FileInputFormat and FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
// writable objects have convenient methods for writing, readning and comparing values
// we could think of the Text class as StringWritable
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LetterCount extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
        // create a new instance of the Job object
        // use the Configured.getConf() method to get the configuration object for this instance of WordCount
        // names the job object wordcount
        Job job = Job.getInstance(getConf(), "wordcount");
 
        // set the jar to use
        job.setJarByClass(this.getClass());
 
        // set the input and outputs paths for your application
        // store input files in HDFS
        // pass the input and output paths as command line arguments at runtime
        FileInputFormat.setInputPaths(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
 
        // set the map and reduce class for the job
        // in this programming example, use the Map and Reduce inner classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        // use a Text object to output the key and the value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // launch the job and wait for it to finish
        // waitForCompletion(boolean verbose)
        // when true the methods reports its progress as the Map and Reduce classes run
        // when false the methods reports progress up to, but not including, and Map and Reduce
        // in Unix, 0 presents success
        return job.waitForCompletion(true) ? 0 : 1;
		//return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
        int res = ToolRunner.run(new LetterCount(), args);
        System.exit(res);
	}
	
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        // define several global variables
        // IntWritable for the value 1, and a Text object to store each word as it is parsed from the input string
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        //  create a regular expression pattern we can use to parse each line of input texton word boundaries
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
 
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
 
            // convert the Text object to a string
            // create a currentword variable, capturing individual words from each input string
            String line = lineText.toString().replaceAll("[^a-zA-Z]"," ");
            Text currentWord = new Text();
 
            // Use the regular expression pattern to split the line into individual words based on word boundaries.
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
 
                // convert the word to lowercase and get the first character of this String
                // store the fist character to currentWord
                if (word.startsWith(" ")) {
                	continue;
                } else {
                    word = word.toLowerCase().substring(0,1);
                    currentWord = new Text(word);
                    context.write(currentWord, one);
                }
 
            }
 
        } // end map
    } // end mapper
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }

}
