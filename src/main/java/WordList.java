import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

/**
 * @className: WordList
 * @description: 获得所有类别的单词列表
 * @description: (input)<Text, IntWritable> -> map -> <Text, IntWritable> -> combine -> <Text, IntWritable> -> reduce -> <Text, IntWritable>(output)
 * @description: (input)<类别@单词, 个数> -> map -> <单词, 个数> -> combine -> <单词, 总个数> -> reduce -> <单词, 总个数>(output)
 * @author: dahongdou
 * @date: 2020/10/21
 **/
public class WordList extends Configured implements Tool {

    public static class WordListMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String word = key.toString().split("@")[1];
            context.write(new Text(word), value);
        }
    }
    public static class WordListReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Statistics word list");

        job.setJarByClass(WordList.class);
        job.setMapperClass(WordListMapper.class);
        job.setCombinerClass(WordListReducer.class);
        job.setReducerClass(WordListReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"output/wordCount", "output/wordList"};
        }
        int res = ToolRunner.run(new Configuration(), new WordList(), args);
        System.exit(res);
    }
}
