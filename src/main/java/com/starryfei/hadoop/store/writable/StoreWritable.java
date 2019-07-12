package com.starryfei.hadoop.store.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClassName: StoreWritable
 * Description: 封装数据
 *
 * @author: starryfei
 * @date: 2019-07-11 11:43
 **/
public class StoreWritable implements Writable {

    private String ip ;
    private String sex ;
    private String birth ;
    private String category ;
    private String time ;
    private String url ;
    private int type;


    public StoreWritable() {
    }

    public StoreWritable(String ip, String sex, String birth, String category, String time, String url) {
        this.ip = ip;
        this.sex = sex;
        this.birth = birth;
        this.category = category;
        this.time = time;
        this.url = url;
    }



    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(sex);
        dataOutput.writeUTF(birth);
        dataOutput.writeUTF(time);
        dataOutput.writeUTF(url);
        dataOutput.writeUTF(category);
        dataOutput.writeInt(type);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ip = dataInput.readUTF();
        sex = dataInput.readUTF();
        birth = dataInput.readUTF();
        time = dataInput.readUTF();
        url =dataInput.readUTF();
        category = dataInput.readUTF();
        type = dataInput.readInt();
    }


    @Override
    public String toString() {
        return "StoreWritable{" +
                "ip='" + ip + '\'' +
                ", sex='" + sex + '\'' +
                ", birth='" + birth + '\'' +
                ", category='" + category + '\'' +
                ", time='" + time + '\'' +
                ", url='" + url + '\'' +
                ", type=" + type +
                '}';
    }
}
