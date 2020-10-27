import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @className: CountTotalWordsOfCategory
 * @description: 统计每个类别的总单词个数
 * @author: dahongdou
 * @date: 2020/10/21
 **/
public class CountTotalWordsOfCategory extends Configured implements Tool {
    public static class CountTotalWordsOfCategoryMapper
            extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String category = key.toString().split("@")[0];
            context.write(new Text(category), value);
        }
    }

    public static class CountTotalWordsOfCategoryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Count the total number of category words");

        job.setJarByClass(CountTotalWordsOfCategory.class);
        job.setMapperClass(CountTotalWordsOfCategoryMapper.class);
        job.setCombinerClass(CountTotalWordsOfCategoryReducer.class);
        job.setReducerClass(CountTotalWordsOfCategoryReducer.class);

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
            args = new String[]{"output/wordcount", "output/totalWordsOfCategory"};
        }
        int res = ToolRunner.run(new Configuration(), new CountTotalWordsOfCategory(), args);
        System.exit(res);
    }
}
