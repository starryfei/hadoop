# hadoop

### wordCout例子

按照官方的例子完成的，

### Store例子

[数据集下载](https://raw.githubusercontent.com/hortonworks/data-tutorials/master/tutorials/hdp/loading-and-querying-data-with-hadoop/assets/retail-store-logs-sample-data.zip)   [来源](https://hortonworks.com/tutorial/loading-and-querying-data-with-hadoop/)

下载解压完的数据格式为tsv,类似csv,里面的数据是按照TAB来分隔的

```
omniture-logs.tsv: 浏览日志(67M)
products.tsv：商品信息(12kb)
users.tsv：用户信息(2M)
```

### 数据格式

#### users.tsv

| SWID（用户ID）                       | BIRTH_DT（出生日期） | GENDER_CD（性别） |
| ------------------------------------ | -------------------- | ----------------- |
| 0001BDD9-EABF-4D0D-81BD-D9EABFCD0D7D | 8-Apr-84             | F                 |

#### products.tsv

| URL                  | category |
| -------------------- | -------- |
| http://www.acme.com/ | Books    |

#### omniture-logs.tsv（数据太多，只选取有用的信息）

| time（1） | IP（7） | URL（12） | UserID（13） |      |
| --------- | ------- | --------- | ------------ | ---- |
|           |         |           |              |      |

### 解决的问题

- 用户访问的商品与性别(年龄)之间的关系
- 用户对那些商品感兴趣
- ….

#### 解决方案

1. 用户访问的商品与性别(年龄)之间的关系

对于该问题，需要进行多文件进行关联，users.id->omniture-log.userid, omniture-log.URL-> product.URL,  考虑到product文件较小，我们可以直接将其加载到内存中，即 通过**setup()**实现，此方法被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作，在**cleanup()**实现资源的清理

剩下的只需要将users和omniture-log文件关联

#### map 任务

对于多文件的输入，有两种方式

```java
// 1多文件输入方式实现
MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,  	TextStoreMap.class);
MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TextStoreMap.class);

```

对于第一种方式，需要分别设计读取不同文件的Map任务。

```java
// 2
FileInputFormat.addInputPath(job, new Path(args[0]));
FileInputFormat.addInputPath(job, new Path(args[1]));
```

第二种方式，只编写一次Map任务，在任务中判断输入的文件是那个，然后进行分别判断,下面的代码实现了获取输入的文件的名字，用于分别是那个文件。

```java
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
            try {
                Method getInputSplitMethod = splitClass
                        .getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        String fileName = fileSplit.getPath().getName();
        return fileName;
    }
```



解决完多文件的输入和识别输入的文件，就需要考虑如何将两个文件进行关联，这就用到Hadoop的shuffle机制，主要是排序，shuffle阶段会将相同的key值放到一起，然后列中的值自然而然的连接到一起，所以将Map结果的key(userId)设置成待连接的列，在reduce阶段取出每个key的List value进行解析，然后用集合分别存储解析到的数据，最后对两个集合求笛卡尔积就行。

#### Map任务

```java
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

```

#### Reduce任务

```java
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
				// 清空数据
        users.clear();
        logs.clear();

    }
```





#### shuffle机制

#### map端shuffle

Tolist……….

#### reduce端shuffle

Tolist……….

a