package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Task1Driver {
    public static void driver(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // jar ****.jar 0path/to/dataSet
        // jar ****.jar 0path/to/dataSet path/to/task1/output
        // jar ****.jar 0path/to/dataSet path/to/task1/output path/to/task2/output
        // jar ****.jar 0path/to/dataSet path/to/task1/output path/to/task2/output path/to/task3/output

        // pathInit
        String datasetPath = args[0];
        String songsPath = datasetPath + "/songs";
        String genresPath = datasetPath + "/genres";
        String lyricsPath = datasetPath + "/lyrics";
        String usersPath = datasetPath + "/users";
        String taskPath = "task1";
        if (args.length > 1) {
            taskPath = args[1];
        }
        String[] subTaskPath = {taskPath + "/songs", taskPath + "/genres", taskPath + "/lyrics", taskPath + "/users"};

        //configuration
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("dictpath", lyricsPath + "/lyric1.txt");

        FileSystem fs = FileSystem.get(conf);

        // Job1_1
        Job job1_1 = Job.getInstance(conf, "task1.Preprocess");

        job1_1.setJarByClass(Preprocess.class);

        job1_1.setOutputKeyClass(Text.class);
        job1_1.setOutputValueClass(Text.class);

        job1_1.setMapperClass(Preprocess.PreprocessMapper.class);
        job1_1.setReducerClass(Preprocess.PreprocessReducer.class);

        job1_1.setInputFormatClass(Preprocess.PreprocessFileInputFormat.class);
        job1_1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1_1, new Path(songsPath));
        FileOutputFormat.setOutputPath(job1_1, new Path(subTaskPath[0]));

        job1_1.waitForCompletion(true);

        Path src = new Path(subTaskPath[0] + "/part-r-00000");
        Path dst = new Path(subTaskPath[0] + "/songs.txt");
        fs.rename(src, dst);

        //job1_2
        Job job1_2 = Job.getInstance(conf, "task1.GenreCount");

        job1_2.setJarByClass(GenreCount.class);

        job1_2.setOutputKeyClass(Text.class);
        job1_2.setOutputValueClass(Text.class);

        job1_2.setMapperClass(GenreCount.GenreCountMapper.class);
        job1_2.setReducerClass(GenreCount.GenreCountReducer.class);

        job1_2.setInputFormatClass(TextInputFormat.class);
        job1_2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job1_2, new Path(genresPath));
        FileInputFormat.addInputPath(job1_2, new Path(subTaskPath[0]));
        FileOutputFormat.setOutputPath(job1_2, new Path(subTaskPath[1]));

        job1_2.waitForCompletion(true);

        src = new Path(subTaskPath[1] + "/part-r-00000");
        dst = new Path(subTaskPath[1] + "/genres.txt");
        fs.rename(src, dst);


        //job1_3
        Job job1_3 = Job.getInstance(conf, "task1.LyricsCount");

        job1_3.setJarByClass(LyricsCount.class);

        job1_3.setOutputKeyClass(Text.class);
        job1_3.setOutputValueClass(Text.class);

        job1_3.setMapperClass(LyricsCount.LyricsCountMapper.class);
        job1_3.setReducerClass(LyricsCount.LyricsCountReducer.class);

        job1_3.setInputFormatClass(TextInputFormat.class);
        job1_3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1_3, new Path(lyricsPath));
        FileInputFormat.addInputPath(job1_3, new Path(subTaskPath[0]));
        FileOutputFormat.setOutputPath(job1_3, new Path(subTaskPath[2]));

        job1_3.waitForCompletion(true);

        src = new Path(subTaskPath[2] + "/part-r-00000");

        dst = new Path(subTaskPath[2] + "/lyrics.txt");
        fs.rename(src, dst);

        //job1_4
        Job job1_4 = Job.getInstance(conf, "task1.UserCount");
        job1_4.setJarByClass(UserCount.class);


        job1_4.setOutputKeyClass(Text.class);
        job1_4.setOutputValueClass(Text.class);

        job1_4.setMapperClass(UserCount.UserCountMapper.class);
        job1_4.setReducerClass(UserCount.UserCountReducer.class);

        job1_4.setInputFormatClass(TextInputFormat.class);
        job1_4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1_4, new Path(usersPath));
        FileInputFormat.addInputPath(job1_4, new Path(subTaskPath[0]));
        FileOutputFormat.setOutputPath(job1_4, new Path(subTaskPath[3]));

        job1_4.waitForCompletion(true);

        src = new Path(subTaskPath[3] + "/part-r-00000");
        dst = new Path(subTaskPath[3] + "/users.txt");
        fs.rename(src, dst);


    }
}
