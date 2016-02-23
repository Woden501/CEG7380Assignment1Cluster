package snedeker.cc.project1.cluster;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

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

public class RunningMean {

	public static class Map extends Mapper<LongWritable, Text, Text, Time_Series> {
		//hadoop supported global variabless
		private Text word = new Text();
		private Time_Series series = new Time_Series();
		    
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//take one line at a time and tokenizing the same
			String line = value.toString();
			
			String[] values = line.split(",");
			// value[0] - company name, value[1] - date, value[2] - price
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
			format.setLenient(false);
			Date date = null;
			try {
				date = format.parse(values[1]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long longDate = date.getTime();
			
			series.set(longDate, Double.parseDouble(values[2]));
			word.set(values[0]);
			context.write(word, series);
			
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			//iterate through all the words available in that line 
//			//and form the key value pair
//			while (tokenizer.hasMoreTokens()) {
//				word.set(tokenizer.nextToken());
//				//send to output collector which passes the same to reducer
//				context.write(word, one);
//			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Time_Series, Text, Text> {

		public void reduce(Text key, Iterable<Time_Series> values, Context context) throws IOException, InterruptedException {
			int seriesCount = 0;
			double size3Total = 0;
			double size4Total = 0;
			
			Text means = new Text();
			
			for (Time_Series series : values) {
				if (seriesCount < 3)
					size3Total += series.getValue();
				
				if (seriesCount < 4)
					size4Total += series.getValue();
				
				++seriesCount;
			}
			
			double mean3 = size3Total / 3.0;
			double mean4 = size4Total / 4.0;
			
			means.set("3 Day: " + mean3 + ", 4 Day: " + mean4);
			
			context.write(key, means);
			
//			int sum = 0;
//			//iterates through all the values with a key and add them together 
//			//and give the final result as the key and sum of its values
//			for (IntWritable val : values) {
//				sum += val.get();
//			}
//			//writes the final result for that word to the reducer context object, 
//			//and moves on to the next
//			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Get configuration object and set a job name
		Configuration conf = new Configuration();
		Job job = new Job(conf, "runningMean");
		job.setJarByClass(snedeker.cc.project1.cluster.RunningMean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Time_Series.class);
		//Set key, output classes for the job (same as output classes for Reducer)
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Set format of input files; "TextInputFormat" views files
		//as a sequence of lines
		job.setInputFormatClass(TextInputFormat.class);
		//Set format of output files: lines of text
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(2); #set num of reducers
		//accept the hdfs input and output directory at run time
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Launch the job and wait for it to finish
		job.waitForCompletion(true);
		
	}
	
}
