package com.starryfei.hadoop.store.text;

import com.starryfei.hadoop.store.Common;
import com.starryfei.hadoop.store.ConfigUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: TextStoreMap
 * Description: {id, {time,ip,url}},存储所有的ID信息
 *
 * @author: starryfei
 * @date: 2019-07-11 10:27
 **/
public class TextStoreMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name = ConfigUtil.getName(context);
        String line = value.toString();
        if ( !line.contains("\t") ) {
            return;

        }
        String[] args = line.split("\t");


        Text id = new Text();
        // 解析users.csv SWID	BIRTH_DT	GENDER_CD
        if ( Common.USER_TSV.replaceAll("#","").equals(name) ) {
            if ( args.length == 3 ) {
                id.set(args[0]);
                StringBuilder sb = new StringBuilder();
                sb.append(Common.USER_TSV+args[1]+" "+args[2]);
                context.write(id, new Text(sb.toString()));
            }
        } else {
            if (!"".equals(args[13]) ) {
                String k = args[13];
                id.set(k.substring(1, k.length() - 1));
                StringBuilder sb = new StringBuilder();
                sb.append(Common.LOG_TSV+args[1]+" "+args[7]+" "+args[12]+" ");
                context.write(id, new Text(sb.toString()));
            }

        }

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // 将加载小文件直接放到内存中products.tsv
    }



}
