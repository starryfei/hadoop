package com.starryfei.hadoop.store.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: TextStoreJob
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-07-11 10:34
 **/
public class TextStoreJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: store file <in> [<in>...] <out>");
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"storetext");
        job.setJarByClass(TextStoreJob.class);
        // 多文件输入方式实现
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, TextStoreMap.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TextStoreMap.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TextStoreReduce.class);
//
//
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//

        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
