import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * @className: CalculateNaiveBayesProbability
 * @description: 计算每个类别每个单词的朴素贝叶斯概率
 * @description: (input)<Text, BytesWritable> -> map -> <Text, DoubleWritable> -> reduce -> <Text, Text>(output)
 * @description: (input)<类别@文件名, 文件内容> -> map -> <类别@文件名@预测类别, 概率> -> reduce -> <类别@文件名, 预测类别>(output)
 * @author: dahongdou
 * @date: 2020/10/21
 **/
public class CalculateNaiveBayesProbability extends Configured implements Tool {

    public static class CalculateNaiveBayesProbabilityMapper extends Mapper<Text, BytesWritable, Text, DoubleWritable> {
        /**
         * wordSet 所有单词集合
         */
        private Set<String> wordSet;
        /**
         * categoryTotalWordsNum 类别的所有单词数目
         * key(String) 类别
         * value(Integer) 单词数目
         */
        private Map<String, Integer> categoryTotalWordsNum;
        /**
         * categorySet 类别集合
         */
        private Set<String> categorySet;
        /**
         * categoryWordProbability 每个类别中每个单词的概率
         * key(String) 类别@单词
         * value(Double) 概率
         *
         * 计算公式：
         */
        private Map<String, Double> categoryWordProbability;
        /**
         * B 总单词类别个数（不重复）
         */
        private int B;
        /**
         * S 总单词个数
         */
        private int S;
        private Map<String, Integer> getMapFromFile(Configuration conf, Path path) throws IOException {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

            Text key = new Text();
            IntWritable value = new IntWritable();
            Map<String, Integer> map = new HashMap<String, Integer>();
            while(reader.next(key, value)) {
                map.put(key.toString(), value.get());
            }
            reader.close();
            return map;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Map<String, Integer> wordCount = getMapFromFile(context.getConfiguration(), new Path(conf.get("wordCount")+"/part-r-00000"));
            categoryTotalWordsNum = getMapFromFile(context.getConfiguration(), new Path(conf.get("categoryTotalWordsNum")+"/part-r-00000"));
            categorySet = categoryTotalWordsNum.keySet();
            wordSet = getMapFromFile(context.getConfiguration(), new Path(conf.get("wordList")+"/part-r-00000")).keySet();
            B = wordSet.size();
            for(Integer num : categoryTotalWordsNum.values()){
                S += num;
            }

            //根据朴素贝叶斯公式计算每个单词的条件概率，以便后面直接使用
            categoryWordProbability = new HashMap<String, Double>();
            for(Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                String category = entry.getKey().split("@")[0];
                Double property = Math.log10((entry.getValue() + 1.0)/(categoryTotalWordsNum.get(category) + B*1.0));

                //乘以先验概率，取log所以是加法运算
                property += Math.log10(categoryTotalWordsNum.get(category) * 1.0) / (S * 1.0);
                categoryWordProbability.put(entry.getKey(), property);
            }
        }

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String content = new String(value.getBytes(), 0, value.getLength());
            StringTokenizer itr = new StringTokenizer(content);
            //String category = key.toString().split("@")[0];
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();

                //forecastCategory预测为哪个类别，每个类别都需要预测得到一个概率。
                for(String forecastCategory : categorySet) {
                    Double probability = 0.0;

                    //用forecastCategory和word组成key，判断训练集这个预测类别是否存在这个单词。是，则直接取概率；否，则设定次数为1.
                    String forecastCategoryWordKey = forecastCategory + "@" + word;
                    if(categoryWordProbability.containsKey(forecastCategoryWordKey)) {
                        probability = categoryWordProbability.get(forecastCategoryWordKey);
                    }else {
                        probability = Math.log10(1.0/(categoryTotalWordsNum.get(forecastCategory) + B*1.0));
                        probability += Math.log10(categoryTotalWordsNum.get(forecastCategory) * 1.0) / (S * 1.0);
                    }
                    context.write(new Text(key.toString() + "@" + forecastCategory), new DoubleWritable(probability));
                }
            }
        }
    }

    public static class CalculateNaiveBayesProbabilityReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        /**
         * forecastResult 预测的结果
         * key(String) 类别@文件
         * value(String) 预测的类别
         */
        private Map<String, String> forecastResult;
        /**
         * forecastMaxNum 预测的最大值（用来控制预测结果，预测的最大值的类别为预测类别）
         * key(String) 类别@文件
         * value(String) 预测概率
         */
        private Map<String, Double> forecastMaxNum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            forecastResult = new HashMap<String, String>();
            forecastMaxNum = new HashMap<String, Double>();
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double probability = 0.0;
            for(DoubleWritable val : values) {
                probability += val.get();
            }

            String categoryFilename = key.toString().split("@")[0] + "@" + key.toString().split("@")[1];
            if(!forecastMaxNum.containsKey(categoryFilename)) {
                forecastMaxNum.put(categoryFilename, probability);
                forecastResult.put(categoryFilename, key.toString().split("@")[2]);
            }else if(forecastMaxNum.get(categoryFilename) < probability) {
                forecastMaxNum.put(categoryFilename, probability);
                forecastResult.put(categoryFilename, key.toString().split("@")[2]);
            }
//            context.write(key, new DoubleWritable(probability));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, String> entry : forecastResult.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Calculate NaiveBayes Probability");

        job.setJarByClass(CalculateNaiveBayesProbability.class);
        job.setMapperClass(CalculateNaiveBayesProbabilityMapper.class);
//        job.setCombinerClass(CalculateNaiveBayesProbabilityReducer.class);
        job.setReducerClass(CalculateNaiveBayesProbabilityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CalculateNaiveBayesProbability(), args);
        System.exit(res);
    }
}
