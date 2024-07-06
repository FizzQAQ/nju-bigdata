package task2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import task1.GenreCount;
import task1.LyricsCount;
import task1.Preprocess;
import task1.UserCount;

import java.io.IOException;

public class Task2Driver {
    public static void driver(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // pathInit
        String inputPath = "task1";
        String taskPath = "task2";
        if(args.length > 1){
            inputPath = args[1];
        }
        if(args.length > 2){
            taskPath = args[2];
        }

        String[] subTaskPath = {taskPath + "/mostPopular", taskPath + "/mostPlay", taskPath + "/duration", taskPath + "/frequency"};

        FileSystem fs = FileSystem.get(conf);

//        // job2_1
//        Job job2_1 = Job.getInstance(conf, "task1.Preprocess");
//
//        job2_1.setJarByClass(Preprocess.class);
//
//        job2_1.setOutputKeyClass(Text.class);
//        job2_1.setOutputValueClass(Text.class);
//
//        job2_1.setMapperClass(Preprocess.PreprocessMapper.class);
//        job2_1.setReducerClass(Preprocess.PreprocessReducer.class);
//
//        job2_1.setInputFormatClass(Preprocess.PreprocessFileInputFormat.class);
//        job2_1.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.addInputPath(job2_1, new Path(songsPath));
//        FileOutputFormat.setOutputPath(job2_1, new Path(subTaskPath[0]));
//
//        job2_1.waitForCompletion(true);
//
//        Path src = new Path(subTaskPath[0] + "/part-r-00000");
//        Path dst = new Path(subTaskPath[0] + "/songs.txt");
//        fs.rename(src, dst);
//
//        // job2_2
//        Job job2_2 = Job.getInstance(conf, "task1.GenreCount");
//
//        job2_2.setJarByClass(GenreCount.class);
//
//        job2_2.setOutputKeyClass(Text.class);
//        job2_2.setOutputValueClass(Text.class);
//
//        job2_2.setMapperClass(GenreCount.GenreCountMapper.class);
//        job2_2.setReducerClass(GenreCount.GenreCountReducer.class);
//
//        job2_2.setInputFormatClass(TextInputFormat.class);
//        job2_2.setOutputFormatClass(TextOutputFormat.class);
//
//
//        FileInputFormat.addInputPath(job2_2, new Path(genresPath));
//        FileInputFormat.addInputPath(job2_2, new Path(subTaskPath[0]));
//        FileOutputFormat.setOutputPath(job2_2, new Path(subTaskPath[1]));
//
//        job2_2.waitForCompletion(true);
//
//        src = new Path(subTaskPath[1] + "/part-r-00000");
//        dst = new Path(subTaskPath[1] + "/genres.txt");
//        fs.rename(src, dst);
//
//
//        // job2_3
//        Job job2_3 = Job.getInstance(conf, "task1.LyricsCount");
//
//        job2_3.setJarByClass(LyricsCount.class);
//
//        job2_3.setOutputKeyClass(Text.class);
//        job2_3.setOutputValueClass(Text.class);
//
//        job2_3.setMapperClass(LyricsCount.LyricsCountMapper.class);
//        job2_3.setReducerClass(LyricsCount.LyricsCountReducer.class);
//
//        job2_3.setInputFormatClass(TextInputFormat.class);
//        job2_3.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.addInputPath(job2_3, new Path(lyricsPath));
//        FileInputFormat.addInputPath(job2_3, new Path(subTaskPath[0]));
//        FileOutputFormat.setOutputPath(job2_3, new Path(subTaskPath[2]));
//
//        job2_3.waitForCompletion(true);
//
//        src = new Path(subTaskPath[2] + "/part-r-00000");
//
//        dst = new Path(subTaskPath[2] + "/lyrics.txt");
//        fs.rename(src, dst);
//
//        // job2_4
//        Job job2_4 = Job.getInstance(conf, "task1.UserCount");
//        job2_4.setJarByClass(UserCount.class);
//
//
//        job2_4.setOutputKeyClass(Text.class);
//        job2_4.setOutputValueClass(Text.class);
//
//        job2_4.setMapperClass(UserCount.UserCountMapper.class);
//        job2_4.setReducerClass(UserCount.UserCountReducer.class);
//
//        job2_4.setInputFormatClass(TextInputFormat.class);
//        job2_4.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.addInputPath(job2_4, new Path(usersPath));
//        FileInputFormat.addInputPath(job2_4, new Path(subTaskPath[0]));
//        FileOutputFormat.setOutputPath(job2_4, new Path(subTaskPath[3]));
//
//        job2_4.waitForCompletion(true);
//
//        src = new Path(subTaskPath[3] + "/part-r-00000");
//        dst = new Path(subTaskPath[3] + "/users.txt");
//        fs.rename(src, dst);


    }
}
