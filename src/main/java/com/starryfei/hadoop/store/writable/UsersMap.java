package com.starryfei.hadoop.store.writable;

import com.starryfei.hadoop.store.writable.StoreWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: d
 * Description: 用作单独处理users.tsv文件
 *
 * @author: starryfei
 * @date: 2019-07-11 10:27
 **/
public class UsersMap extends Mapper<LongWritable, Text, Text, StoreWritable> {
    private int count = 0;
    private Text id = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if ( !line.contains("\t") ) {
            return;

        }
        String[] args = line.split("\t");
        StoreWritable storeWritable = new StoreWritable();
        // 解析users.csv SWID	BIRTH_DT	GENDER_CD
        if ( args.length > 3 ) {
            id.set(args[0]);
            storeWritable.setBirth(args[1]);
            storeWritable.setSex(args[2]);
            storeWritable.setType(1);
            context.write(id, storeWritable);
        }
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }


}
