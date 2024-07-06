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

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Job1

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator",",");
        conf.set("dictpath",args[2]+"/lyric1.txt");
        Job job1 = Job.getInstance(conf, "Preprocess");
        job1.setJarByClass(Preprocess.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Preprocess.PreprocessMapper.class);
        job1.setReducerClass(Preprocess.PreprocessReducer.class);
        job1.setInputFormatClass(Preprocess.PreprocessFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[4] + "/output0"));
        job1.waitForCompletion(true);
        FileSystem fs=FileSystem.get(conf);
        Path src=new Path(args[4]+"/output0/part-r-00000");
        Path dst=new Path(args[4]+"/output0/songs.txt");
        fs.rename(src,dst);
//        //job2

        Job job2 = Job.getInstance(conf, "genreCount");
        job2.setJarByClass(genreCount.class);
        job2.setInputFormatClass(TextInputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(genreCount.genreCountMapper.class);
        job2.setReducerClass(genreCount.genreCountReducer.class);

        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileInputFormat.addInputPath(job2,new Path(args[4]+"/output0"));
        FileOutputFormat.setOutputPath(job2, new Path(args[4] + "/output1"));
        job2.waitForCompletion(true);
        src=new Path(args[4]+"/output1/part-r-00000");
        dst=new Path(args[4]+"/output1/genres.txt");
        fs.rename(src,dst);
//        //job3

        Job job3 = Job.getInstance(conf, "LyricsCount");
        job3.setJarByClass(LyricsCount.class);
        job3.setInputFormatClass(TextInputFormat.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(LyricsCount.LyricsCountMapper.class);
        job3.setReducerClass(LyricsCount.LyricsCountReducer.class);

        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileInputFormat.addInputPath(job3,new Path(args[4]+"/output0"));
        FileOutputFormat.setOutputPath(job3, new Path(args[4] + "/output2"));
        //job4
        job3.waitForCompletion(true);
        src=new Path(args[4]+"/output2/part-r-00000");
        dst=new Path(args[4]+"/output2/lyrics.txt");
        fs.rename(src,dst);
        Job job4 = Job.getInstance(conf, "UserCount");
        job4.setJarByClass(UserCount.class);
        job4.setInputFormatClass(TextInputFormat.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setMapperClass(UserCount.UserCountMapper.class);
        job4.setReducerClass(UserCount.UserCountReducer.class);

        job4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileInputFormat.addInputPath(job4,new Path(args[4]+"/output0"));
        FileOutputFormat.setOutputPath(job4, new Path(args[4] + "/output3"));
        job4.waitForCompletion(true);
        src=new Path(args[4]+"/output3/part-r-00000");
        dst=new Path(args[4]+"/output3/users.txt");
        fs.rename(src,dst);
    }
}