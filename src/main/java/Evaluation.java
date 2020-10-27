import com.sun.org.apache.bcel.internal.classfile.ElementValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.HashMap;
import java.util.Map;

/**
 * @className: Evaluation
 * @description: 评估分类效果，三个指标precision, recall, F1
 * @author: dahongdou
 * @date: 2020/10/23
 **/
public class Evaluation extends Configured implements Tool {
    public static class EvaluationMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        /**
         * TP（实际为正样本预测为正样本）、FN（实际为正样本预测为负样本）、FP（实际为负样本预测为正样本）
         * key（String） 类别
         * value（Integer） 个数
         */
        private Map<String, Integer> TP, FN, FP;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            TP = new HashMap<String, Integer>();
            FN = new HashMap<String, Integer>();
            FP = new HashMap<String, Integer>();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //真实类别和预测类别
            String category = key.toString().split("@")[0];
            String forecastCategory = value.toString();

            //设a为真实类别，b为预测类别。
            //若a==b，TP[a]++；
            //若a!=b，FN[a]++、FP[b]++；
            if(category.equals(forecastCategory)) {
                try {
                    TP.put(category, TP.get(category) + 1);
                }catch (Exception e) {
                    TP.put(category, 1);
                }
            }else {
                try {
                    FN.put(category, FN.get(category) + 1);
                }catch (Exception e) {
                    FN.put(category, 1);
                }
                try {
                    FP.put(forecastCategory, FP.get(forecastCategory) + 1);
                }catch (Exception e) {
                    FP.put(forecastCategory, 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(String key : TP.keySet()) {
                double tp = TP.get(key);
                double fn = FN.containsKey(key) ? FN.get(key) : 0;
                double fp = FP.containsKey(key) ? FP.get(key) : 0;

                //计算precision、recall、F1三个指标
                double precision = tp / (tp + fp);
                double recall = tp / (tp + fn);
                double F1 = 2*tp / (2*tp + fp + fn);

                context.write(new Text("precision"), new DoubleWritable(precision));
                context.write(new Text("recall"), new DoubleWritable(recall));
                context.write(new Text("F1"), new DoubleWritable(F1));
            }
        }
    }

    public static class EvaluationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int num = 0;
            for(DoubleWritable val : values){
                sum += val.get();
                num++;
            }
            context.write(key, new DoubleWritable(sum / num));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Evaluation");

        job.setJarByClass(Evaluation.class);
        job.setMapperClass(EvaluationMapper.class);
        job.setCombinerClass(EvaluationReducer.class);
        job.setReducerClass(EvaluationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Evaluation(), args);
        System.exit(res);
    }
}
