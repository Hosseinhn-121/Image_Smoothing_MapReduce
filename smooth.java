package smooth;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;  
import java.util.HashMap;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.common.IOUtils;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.Length;

import java.math.BigInteger; 

public class smooth {
//	start of smooth

	
//    mapper_tokenizer class
public static class mapper_tokenizer extends Mapper<Object, Text, Text, IntWritable> {

	public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
		
		StringTokenizer itr = new StringTokenizer(value.toString(),"-");
		List <String> first_token = new ArrayList();
		List <String> x_y = new ArrayList();
		HashMap<String, String> x_y_rgb = new HashMap<String, String>();
		
		while (itr.hasMoreTokens()) {
			first_token.add(itr.nextToken());
		}

		for(int i = 0;i < first_token.size();i++) {
			String [] a = new String[2];
			a = (first_token.get(i)).split("!");
			x_y.add(a[0]);
			x_y_rgb.put(a[0], a[1]);
		}
		String [] d = new String[2];
		d = (x_y.get(x_y.size()-1)).split(" ");
		int x_size = Integer.parseInt(d[0]) + 1;
		int y_size = Integer.parseInt(d[1]) + 1;
		
		for(int i = 0; i < x_y.size() ;i++) {
			String [] a = new String[2];
			int [] b = new int[2];
			a = (x_y.get(i)).split(" ");
			b[0] = Integer.parseInt(a[0]);
			b[1] = Integer.parseInt(a[1]);
			for(int j = -1; j < 2 ; j++) {
				for(int z = -1; z < 2 ; z++) {
					
					Text textKey = new Text();
					if(b[0]+z != -1 && b[1]+z != -1 && b[0]+z != x_size && b[1]+z != y_size && b[0]+j != -1 &&
							b[1]+j != -1 && b[0]+j != x_size && b[1]+j != y_size) {
						IntWritable RGB = new IntWritable(Integer.parseInt(x_y_rgb.get(Integer.toString(b[0]+j)+" "+Integer.toString(b[1]+z)),16));
						textKey.set(Integer.toString(b[0])+" "+Integer.toString(b[1]));
						context.write(textKey, RGB);
					}else {
						IntWritable RGB = new IntWritable(0);
						textKey.set(Integer.toString(b[0])+" "+Integer.toString(b[1]));
						context.write(textKey, RGB);
					}
				}
			}
		}
	}
}
//    end mapper_tokenizer class

//	reducer_sum class
public static class reducer_sum extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Text textValue = new Text();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int r = 0;
		int g = 0;
		int b = 0;
		for(IntWritable val : values) {
	        int RGB = val.get();
	        r += (RGB & 0xFF0000) >> 16;
	        g += (RGB & 0xFF00) >> 8;
	        b += (RGB & 0xFF);
			
		}
		float r_av= 0;
		float g_av= 0;
		float b_av= 0;
		r_av = r / 9;
		g_av = g / 9;
		b_av = b / 9;
		int r_av_int = (int) Math.round(r_av);
		int g_av_int = (int) Math.round(g_av);
		int b_av_int = (int) Math.round(b_av);
        String a = String.format("%02x%02x%02x", r_av_int, g_av_int, b_av_int);
        IntWritable RGB = new IntWritable(Integer.parseInt(a,16));
		context.write(key, RGB);

	}

}
//	end reducer_sum class
public static void main(String[] args) throws Exception {
	JobConf jconf = new JobConf(smooth.class);
	Job job = Job.getInstance(jconf, "smooth");
	job.setJarByClass(smooth.class);
	job.setMapperClass(mapper_tokenizer.class);
	job.setCombinerClass(reducer_sum.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
	
	
	
//	end of smooth

}
