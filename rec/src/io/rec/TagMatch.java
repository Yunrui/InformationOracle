package io.rec;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagMatch implements Tool {

    public static final Logger log = LoggerFactory.getLogger(TagMatch.class);
    Configuration configuration;

    public static class MyMap extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
            String stuInfo = value.toString();//将输入的纯文本的数据转换成String
            System.out.println("studentInfo:"+stuInfo);
            log.info("MapSudentInfo:"+stuInfo);
            //将输入的数据先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(stuInfo, "\n");
            //分别对每一行进行处理
            while(tokenizerArticle.hasMoreTokens()){
                // 每行按空格划分
                StringTokenizer tokenizer = new StringTokenizer(tokenizerArticle.nextToken());
                String name = tokenizer.nextToken();//学生姓名
                String score = tokenizer.nextToken();//学生成绩
                Text stu = new Text(name);
                int intscore = Integer.parseInt(score);
                log.info("MapStu:"+stu.toString()+" "+intscore);
                context.write(stu,new IntWritable(intscore));//输出学生姓名和成绩
            }
        }

    }

    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,Context context)
                throws IOException, InterruptedException {
            int sum=0;
            int count=0;
            Iterator<IntWritable> iterator=    values.iterator();
            while(iterator.hasNext()){
                sum+=iterator.next().get();//计算总分
                count++;//统计总科目
            }
            int avg= (int)sum/count;
            context.write(key,new  IntWritable(avg));//输出学生姓名和平均值
        }

    }

    public  int run(String [] args) throws Exception{

        Job job = new Job(getConf());
        job.setJarByClass(TagMatch.class);
        job.setJobName("tagMatch");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMap.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));//设置输入文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//设置输出文件路径
        boolean success=  job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //在eclipse 工具上配置输入和输出参数
        int ret = ToolRunner.run(new TagMatch(), args);
        System.exit(ret);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        conf = new Configuration();
        configuration=conf;
    }
}

