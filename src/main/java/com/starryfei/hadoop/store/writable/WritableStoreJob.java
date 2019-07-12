package com.starryfei.hadoop.store.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * ClassName: TextStoreJob
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-07-11 10:34
 **/
public class WritableStoreJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: store file <in> [<in>...] <out>");
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"store");
        job.setJarByClass(WritableStoreJob.class);
        job.setMapperClass(WritableStoreMap.class);
        job.setMapOutputValueClass(StoreWritable.class);

        job.setReducerClass(WritableStoreReduce.class);
//
//
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StoreWritable.class);
//


        FileInputFormat.addInputPath(job, new Path(args[0]));
//
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
