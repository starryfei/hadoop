package com.starryfei.hadoop.store;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * ClassName: ConfigUtil
 * Description: TODO
 *
 * @author: starryfei
 * @date: 2019-07-11 23:24
 **/
public class ConfigUtil {
    /**
     * 根据分片确认输入的文件名字
     * @param context
     * @return
     * @throws IOException
     */
    public static String getName(Mapper.Context context) throws IOException {
        InputSplit split = context.getInputSplit();
        //String fileName=((FileSplit)inputSplit).getPath().getName();
        Class<? extends InputSplit> splitClass = split.getClass();

        FileSplit fileSplit = null;
        if ( splitClass.equals(FileSplit.class) ) {
            fileSplit = (FileSplit) split;
        } else if ( splitClass.getName().equals(
                "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit") ) {
            // begin reflection hackery...
            try {
                Method getInputSplitMethod = splitClass
                        .getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                // wrap and re-throw error
                throw new IOException(e);
            }
            // end reflection hackery
        }
        String fileName = fileSplit.getPath().getName();
        return fileName;
    }
}
