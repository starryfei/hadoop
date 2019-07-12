package com.starryfei.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: WordCoutTest
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-06-30 16:38
 **/
public class WordCoutTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(WordCoutTest.class);

        job.setMapperClass(MapperTest.class);
        job.setCombinerClass(ReduceTest.class);
        job.setReducerClass(ReduceTest.class);

//        job.setInputFormatClass(XmlInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}
