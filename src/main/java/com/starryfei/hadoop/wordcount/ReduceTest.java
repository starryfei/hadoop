package com.starryfei.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: ReduceTest
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-06-30 16:32
 **/
public class ReduceTest extends Reducer<Text, IntWritable,Text, IntWritable > {
    private  IntWritable res = new IntWritable();

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val:values
             ) {
            count += val.get();
        }

        res.set(count);
        context.write(key,res);
    }
}
