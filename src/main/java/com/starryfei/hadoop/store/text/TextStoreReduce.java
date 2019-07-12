package com.starryfei.hadoop.store.text;

import com.starryfei.hadoop.store.Common;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ClassName: TextStoreReduce
 * Description: 利用shuffle过程会将相同的key进行组合的特性实现
 *
 * @author: starryfei
 * @date: 2019-07-11 11:05
 **/
public class TextStoreReduce extends Reducer<Text, Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 存放User.tsv
        List<String> users = new ArrayList<>();
        List<String> logs = new ArrayList<>();
        values.forEach(v ->{
            if(v.toString().startsWith(Common.USER_TSV)) {
                users.add(v.toString().replace(Common.USER_TSV,""));
            } else {
                logs.add(v.toString().replace(Common.LOG_TSV,""));
            }
        });

        users.forEach(user->logs.forEach(log->{
            Text text = new Text();
            text.set(user+" "+log);
            try {
                context.write(new Text(key),text);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        users.clear();
        logs.clear();

    }
}
