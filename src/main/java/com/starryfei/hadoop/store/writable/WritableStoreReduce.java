package com.starryfei.hadoop.store.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: TextStoreReduce
 * Description: 利用shuffle过程会将相同的key进行组合的特性实现
 *
 * @author: starryfei
 * @date: 2019-07-11 11:05
 **/
public class WritableStoreReduce extends Reducer<Text, StoreWritable,Text,StoreWritable> {
    @Override
    protected void reduce(Text key, Iterable<StoreWritable> values, Context context) throws IOException, InterruptedException {
        // 存放User.tsv
        List<StoreWritable> users = new ArrayList<>();
        List<StoreWritable> logs = new ArrayList<>();
        values.forEach(v -> {
            if (v.getType() == 1) {
               users.add(v);
            } else if ( v.getType() == 2 ){
                logs.add(v);
            }
        });

        if(users.size()>0 && logs.size()>0) {
            for (StoreWritable user: users) {
                for (StoreWritable log : logs) {
                    StoreWritable result = new StoreWritable();
                    result.setIp(log.getIp());
                    result.setUrl(log.getUrl());
                    result.setTime(log.getTime());
                    result.setBirth(user.getBirth());
                    result.setSex(user.getSex());
                    result.setCategory("");
                    context.write(key, result);
                }

            }

        }
        users.clear();
        logs.clear();

    }
}
