package com.starryfei.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: MapperTest
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-06-28 18:56
 **/
public class MapperTest extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       IntWritable count = new IntWritable(1);
        String[] lines = value.toString().split(" ");
        for (String line:lines
             ) {
            context.write(new Text(line),count);
        }
    }
}
