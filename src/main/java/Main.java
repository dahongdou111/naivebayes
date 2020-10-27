import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * @className: Main
 * @description: 整个朴素贝叶斯算法的启动类
 * @author: dahongdou
 * @date: 2020/10/22
 **/
public class Main {
    /**
     * trainFiles 训练文件集合
     * testFiles 测试文件集合
     */
    private static List<String> trainFiles = new ArrayList<String>(), testFiles = new ArrayList<String>();
    /**
     * proportion 训练集与测试集比例
     */
    private static double proportion = 0.8;
    /**
     * outputPath 输出路径（由输入参数的最后一个给出）
     */
    private static String outputPath;

    private static void randomSplitFiles(FileSystem fs, Path dirPath) {
        try {
            //当前目录下总的文件集合
            FileStatus[] status = fs.listStatus(dirPath);

            //训练集个数
            int trainFilesNum = (int)(status.length * proportion);

            //随机获得训练集下标
            Set<Integer> trainFilesIndex = new HashSet<Integer>();
            Random random = new Random();
            while(trainFilesIndex.size() < trainFilesNum) {
                trainFilesIndex.add(random.nextInt(status.length));
            }

            //分别将文件路径加入训练集和测试集
            for(int i=0; i<status.length; i++){
                if(trainFilesIndex.contains(i)){
                    trainFiles.add(status[i].getPath().toString());
                }else{
                    testFiles.add(status[i].getPath().toString());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 合并两个路径，没完成，先这样吧。。。
     * @param path1
     * @param path2
     * @return
     */
    private static String mergePath(String path1, String path2) {
        return path1 + path2;
//        return Path.mergePaths(new Path(path1), new Path(path2)).toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("请输入类别");
            return;
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        //将所有的类别按照proportion比例将文件随机划分
        for(int i=0; i<args.length-1; i++){
            randomSplitFiles(fs, new Path(args[i]));
        }

        //设置输出路径为args的最后一个参数
        outputPath = args[args.length-1];

        //将小文件打包成sequenceFile
        trainFiles.add(mergePath(outputPath, "trainSequenceFile"));
        ToolRunner.run(conf, new MySequenceFile(), trainFiles.toArray(new String[0]));

        testFiles.add(mergePath(outputPath, "testSequenceFile"));
        ToolRunner.run(conf, new MySequenceFile(), testFiles.toArray(new String[0]));

        //统计每个类别每个单词出现的次数
        ToolRunner.run(conf, new WordCount(), new String[]{mergePath(outputPath, "trainSequenceFile"), mergePath(outputPath, "wordCount")});

        //统计每个类别的单词总数
        ToolRunner.run(conf, new CountTotalWordsOfCategory(), new String[]{mergePath(outputPath, "wordCount"), mergePath(outputPath, "categoryTotalWordsNum")});

        //统计单词列表
        ToolRunner.run(conf, new WordList(), new String[]{mergePath(outputPath, "wordCount"), mergePath(outputPath, "wordList")});

        conf.set("wordCount", mergePath(outputPath, "wordCount"));
        conf.set("wordList", mergePath(outputPath, "wordList"));
        conf.set("categoryTotalWordsNum", mergePath(outputPath, "categoryTotalWordsNum"));

        //预测分类结果
        ToolRunner.run(conf, new CalculateNaiveBayesProbability(), new String[]{mergePath(outputPath, "testSequenceFile"), mergePath(outputPath, "probability")});

        //评估
        ToolRunner.run(conf, new Evaluation(), new String[]{mergePath(outputPath, "probability"), mergePath(outputPath, "evaluation")});
    }
}
