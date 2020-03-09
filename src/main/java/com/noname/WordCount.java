package com.noname;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2020/3/6.
 * <p>
 * 使用MapReduce 开发wordCount应用程序
 * 功能举例：此处有一个字符串 a b c a a a a b b c
 * Mapper首先将字符拆分成类似a:1,b:1,c:1,a:1,a:1,a:1,a:1,b:1,b:1,c:1
 * Reduce负责将这些拆开的字符聚合统计，相当于将a,b,c的值相加，即a:5,b:3,c:2
 * <p>
 * 此程序需要打包成jar,通过yarn来运行,需要先在服务器上新建一个hello.txt,文件内容为a b c a a a a b b c，
 * 并且需要将该文件上传到Hadoop服务器上，即通过指令hadoop fs -put [file] [Path]上传，启动yarn后，
 * 可以使用指令hadoop jar XXXX.jar main方法所在类的路径 参数*N调用，此处运行为：
 * hadoop jar hadoop-1.0-SNAPSHOT.jar com.noname.WordCount /share/hello.txt /share/output
 */
public class WordCount {

    /**
     * Mapper中的4个泛型，前两个分别是输入的建类型，和输入的值类型，后两个对应的是输出的键类型和输出的值类型
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接收到的每一行数据
            String line = value.toString();
            //按照指定分隔符进行拆分
            String[] words = line.split(" ");
            for (String word : words) {
                //将Map的处理结果输出，此输出也是Reduce的输入,此处输出的类似于a:1,b:1,c:1,a:1,a:1,a:1,a:1,b:1,b:1,c:1
                context.write(new Text(word), one);
            }
        }
    }


    /**
     * Reducer的泛型和Mapper的意义是相同的，只不过Mapper的输出相当于是Reduce的输入，所以Mapper的后两个泛型
     * 就是Reduce的输入类型，而Reduce的输出类型则根据需求确定，此处输出的结果我们希望类似于 a:1,b:2,c:3
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            //最终的统计结果输出，类似于a:5,b:3,c:2
            context.write(key, new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //创建作业，第二个参数可以自定义，作为job的名称
        Job job = Job.getInstance(configuration, "wordcount");
        //清空已存在的输出文件，因为hadoop不支持自动替换，若输出文件已经存在会抛出异常
        Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println("删除输出文件成功！");
        }

        //设置JOB的处理类
        job.setJarByClass(WordCount.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置map相关参数(输出key类型，输出value类型)
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置Combiner,其实可以不加，但是加了以后可以提升效率，因为在Mapper之后会先在本地对Mapper做一次Reduce,参考文档
        job.setCombinerClass(MyReducer.class);

        //设置Reduce相关参数(输出key类型，输出value类型)
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}