package task3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task3Driver {
    public static void driver(Configuration conf, String[] args) throws Exception {
        Job job3_1 = Job.getInstance(conf, "task3.TFIDF");
        job3_1.setJarByClass(TFIDF.class);
        job3_1.setOutputKeyClass(Text.class);
        job3_1.setOutputValueClass(Text.class);
        job3_1.setMapperClass(TFIDF.Mapper.class);
        job3_1.setReducerClass(TFIDF.Reducer.class);
        job3_1.setInputFormatClass(TextInputFormat.class);
        job3_1.setOutputFormatClass(TextOutputFormat.class);
        String task1Path = "task1";
        if (args.length > 1)
            task1Path = args[1];
        task1Path += "/lyrics";
        // task1Path = "proj-out-5/lyrics"; // hard-coded
        FileInputFormat.addInputPath(job3_1, new Path(task1Path));
        String task3Path = "task3";
        if (args.length > 3)
            task3Path = args[3];
        FileOutputFormat.setOutputPath(job3_1, new Path(task3Path + "/tfidf"));
        // get track count
        Path lyricPath = new Path(task1Path + "/lyrics.txt");
        FileSystem fs = FileSystem.get(conf);
        int trackCount = 0;
        try (FSDataInputStream inputStream = fs.open(lyricPath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            while (reader.readLine() != null) {
                trackCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        job3_1.getConfiguration().setInt("trackCount", trackCount);
        job3_1.waitForCompletion(true);

        Job job3_2 = Job.getInstance(conf, "task3.Sort");
        job3_2.setJarByClass(Sort.class);
        job3_2.setOutputKeyClass(Text.class);
        job3_2.setOutputValueClass(Text.class);
        job3_2.setMapperClass(Sort.Mapper.class);
        job3_2.setReducerClass(Sort.Reducer.class);
        job3_2.setInputFormatClass(TextInputFormat.class);
        job3_2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job3_2, new Path(task3Path + "/tfidf"));
        FileOutputFormat.setOutputPath(job3_2, new Path(task3Path + "/sorted"));
        job3_2.waitForCompletion(true);

        Job job3_3 = Job.getInstance(conf, "task3.KNN");
        job3_3.setJarByClass(KNN.class);
        job3_3.setOutputKeyClass(Text.class);
        job3_3.setOutputValueClass(Text.class);
        job3_3.setMapperClass(KNN.KNNMapper.class);
        job3_3.setReducerClass(org.apache.hadoop.mapreduce.Reducer.class);
        job3_3.setInputFormatClass(TextInputFormat.class);
        job3_3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job3_3, new Path(task3Path + "/sorted/part-r-00000"));
        FileOutputFormat.setOutputPath(job3_3, new Path(task3Path + "/knn"));

        job3_3.getConfiguration().set("pretrainedData", args[0] + "/sentiment_train.txt");
        job3_3.getConfiguration().set("lyrics", task3Path + "/sorted/part-r-00000");

        job3_3.waitForCompletion(true);
    }

}
