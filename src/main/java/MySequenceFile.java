import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @className: MySequenceFile
 * @description: 将小文件打包成sequenceFile。
 * @description: (input)<NullWritable, BytesWritable> -> map -> <Text, BytesWritable>(output)
 * @description: (input)<null, 文件内容> -> map -> <类别@文件名, 文件内容>(output)
 * @author: dahongdou
 * @date: 2020/10/21
 **/
public class MySequenceFile extends Configured implements Tool {

    static class FileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        /**
         * 写入sequenceFile的小文件的key值，形式为“类别@文件名”
         */
        private Text filenameKey;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            String filename = path.getName();
            String classname = path.getParent().getName();
            filenameKey = new Text(classname + "@" + filename);
            //System.out.println(classname + "@" + filename);
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(filenameKey, value);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "SequenceFile");

        job.setJarByClass(MySequenceFile.class);
        job.setMapperClass(FileMapper.class);

        job.setInputFormatClass(MyFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //最后一个参数为输出路径
        for(int i=0; i<args.length-1; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MySequenceFile(), args);
        System.exit(res);
    }
}
