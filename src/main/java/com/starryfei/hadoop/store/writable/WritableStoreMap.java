package com.starryfei.hadoop.store.writable;

import com.starryfei.hadoop.store.Common;
import com.starryfei.hadoop.store.ConfigUtil;
import com.starryfei.hadoop.store.writable.StoreWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * ClassName: TextStoreMap
 * Description: {id, {time,ip,url}}
 *
 * @author: starryfei
 * @date: 2019-07-11 10:27
 **/
public class WritableStoreMap extends Mapper<LongWritable, Text, Text, StoreWritable> {
    private int count = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name = ConfigUtil.getName(context);
        String line = value.toString();
        if ( !line.contains("\t") ) {
            return;

        }
        String[] args = line.split("\t");

        StoreWritable storeWritable = new StoreWritable();
        Text id = new Text();
        // 解析users.csv SWID	BIRTH_DT	GENDER_CD
        if ( Common.USER_TSV.replace("#","").equals(name) ) {
            if ( args.length == 3 ) {
                id.set(args[0]);
                storeWritable.setTime("");
                storeWritable.setUrl("");
                storeWritable.setIp("");
                storeWritable.setCategory("");
                storeWritable.setBirth(args[1]);
                storeWritable.setSex(args[2]);
                storeWritable.setType(1);
                context.write(id, storeWritable);
            }
        } else {
            String k = args[13];

            if (!"".equals(k) ) {
                id.set(k.substring(1, k.length() - 1));
                storeWritable.setTime(args[1]);
                storeWritable.setIp(args[7]);
                storeWritable.setUrl(args[12]);
                storeWritable.setType(2);
                storeWritable.setSex("");
                storeWritable.setBirth("");
                storeWritable.setCategory("");
                context.write(id, storeWritable);
            }

        }

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }


}
