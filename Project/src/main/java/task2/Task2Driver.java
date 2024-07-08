package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task2Driver {

    public static void driver(Configuration conf, String[] args) throws Exception {
        // pathInit
        String inputPath = "task1";
        String taskPath = "task2";
        if (args.length > 1) {
            inputPath = args[1];
        }
        if (args.length > 2) {
            taskPath = args[2];
        }

        String[] subInputPath = {inputPath + "/songs", inputPath + "/genres", inputPath + "/lyrics", inputPath + "/users"};
        String[] subTaskPath = {taskPath + "/mostPopular", taskPath + "/mostPlay", taskPath + "/duration", taskPath + "/frequency", taskPath + "/genreMining"};
        FileSystem fs = FileSystem.get(conf);
        Path src;
        Path dst;
        conf.set("mapred.textoutputformat.separator", ",");
        // job2_1 统计最受欢迎的十首歌曲
        Job job2_1 = Job.getInstance(conf, "task2.MostPopular");

        job2_1.setJarByClass(MostPopular.class);

        job2_1.setOutputKeyClass(Text.class);
        job2_1.setOutputValueClass(Text.class);

        job2_1.setMapperClass(MostPopular.MostPopularMapper.class);
        job2_1.setReducerClass(MostPopular.MostPopularReducer.class);

        job2_1.setInputFormatClass(TextInputFormat.class);
        job2_1.setOutputFormatClass(TextOutputFormat.class);
        // 统计最受欢迎的十首歌曲 输入 songs-从id转化为歌曲名称 与 users-获取歌曲播放次数
        FileInputFormat.addInputPath(job2_1, new Path(subInputPath[0]));
        FileInputFormat.addInputPath(job2_1, new Path(subInputPath[3]));
        FileOutputFormat.setOutputPath(job2_1, new Path(subTaskPath[0]));

        job2_1.waitForCompletion(true);

        src = new Path(subTaskPath[0] + "/part-r-00000");
        dst = new Path(subTaskPath[0] + "/task21.txt");
        fs.rename(src, dst);

        // job2_2
        Job job2_2 = Job.getInstance(conf, "task2.MostPlay");

        job2_2.setJarByClass(MostPlay.class);

        job2_2.setOutputKeyClass(Text.class);
        job2_2.setOutputValueClass(Text.class);

        job2_2.setMapperClass(MostPlay.MostPlayMapper.class);
        job2_2.setReducerClass(MostPlay.MostPlayReducer.class);

        job2_2.setInputFormatClass(TextInputFormat.class);
        job2_2.setOutputFormatClass(TextOutputFormat.class);
        // 统计听歌最多的用户和播放次数最多的艺术家 输入 songs-获取艺术家id 与 users-获取用户歌曲播放次数
        FileInputFormat.addInputPath(job2_2, new Path(subInputPath[0]));
        FileInputFormat.addInputPath(job2_2, new Path(subInputPath[3]));
        FileOutputFormat.setOutputPath(job2_2, new Path(subTaskPath[1]));

        job2_2.waitForCompletion(true);

        src = new Path(subTaskPath[1] + "/part-r-00000");
        dst = new Path(subTaskPath[1] + "/task22.txt");
        fs.rename(src, dst);

        // job2_3 统计歌曲时间并绘制直方图
        Job job2_3 = Job.getInstance(conf, "task2.DurationStatistics");

        job2_3.setJarByClass(DurationStatistics.class);

        job2_3.setOutputKeyClass(Text.class);
        job2_3.setOutputValueClass(Text.class);

        job2_3.setMapperClass(DurationStatistics.DurationStatisticsMapper.class);
        job2_3.setReducerClass(DurationStatistics.DurationStatisticsReducer.class);

        job2_3.setInputFormatClass(TextInputFormat.class);
        job2_3.setOutputFormatClass(TextOutputFormat.class);
        // 统计songs中每首歌曲的duration
        FileInputFormat.addInputPath(job2_3, new Path(subInputPath[0]));
        FileOutputFormat.setOutputPath(job2_3, new Path(subTaskPath[2]));

        job2_3.waitForCompletion(true);

        src = new Path(subTaskPath[2] + "/part-r-00000");
        dst = new Path(subTaskPath[2] + "/task23.txt");
        fs.rename(src, dst);
        // 绘制直方图
        drawPng(fs,3 , dst , new Path(subTaskPath[2] + "/task23.png") );


        // Task24执行两个mapreduceJob,分别为GenreLyrics FrequencyStatistics
        // job2_4_1 将通过track_id联系的Genre Lyrics改为Genre-Lyrics键值对
        Job job2_4_1 = Job.getInstance(conf, "task2.GenresLyrics");
        job2_4_1.setJarByClass(FrequencyStatistics.class);

        job2_4_1.setOutputKeyClass(Text.class);
        job2_4_1.setOutputValueClass(Text.class);

        job2_4_1.setMapperClass(FrequencyStatistics.GenreLyricsMapper.class);
        job2_4_1.setReducerClass(FrequencyStatistics.GenreLyricsReducer.class);

        job2_4_1.setInputFormatClass(TextInputFormat.class);
        job2_4_1.setOutputFormatClass(TextOutputFormat.class);
        // genres-获取track_id与其genre，lyrics-获取track_id与其歌词
        FileInputFormat.addInputPath(job2_4_1, new Path(subInputPath[1]));
        FileInputFormat.addInputPath(job2_4_1, new Path(subInputPath[2]));
        FileOutputFormat.setOutputPath(job2_4_1, new Path(subTaskPath[3] + "_temp"));

        job2_4_1.waitForCompletion(true);

        src = new Path(subTaskPath[3] + "_temp/part-r-00000");

        // job2_4_2 根据job2_4_1得到的Genre-Lyrics键值对，计算其词频
        Job job2_4_2 = Job.getInstance(conf, "task2.FrequencyStatistics");
        // 歌曲流派字符串
        String[] genres = {"Rock", "Metal", "Pop", "Country", "Rap", "Electronic", "Reggae", "Punk", "RnB", "Jazz", "Blues", "Folk", "Latin", "World", "NewAge"};

        job2_4_2.setJarByClass(FrequencyStatistics.class);

        job2_4_2.setOutputKeyClass(Text.class);
        job2_4_2.setOutputValueClass(Text.class);

        job2_4_2.setMapperClass(FrequencyStatistics.FrequencyStatisticsMapper.class);
        job2_4_2.setReducerClass(FrequencyStatistics.FrequencyStatisticsReducer.class);

        job2_4_2.setInputFormatClass(TextInputFormat.class);
        job2_4_2.setOutputFormatClass(TextOutputFormat.class);
        // 设置多文件输入
        for (String genre : genres) {
            MultipleOutputs.addNamedOutput(job2_4_2, genre, TextOutputFormat.class, Text.class, Text.class);
        }
        // 输入为job2_4_1的输出文件
        FileInputFormat.addInputPath(job2_4_2, src);
        FileOutputFormat.setOutputPath(job2_4_2, new Path(subTaskPath[3] + "/task24"));

        job2_4_2.waitForCompletion(true);

        for (String genre : genres) {
            src = new Path(subTaskPath[3] + "/task24/" + genre + "-r-00000");
            dst = new Path(subTaskPath[3] + "/task24/" + genre + ".txt");
            fs.rename(src, dst);
        }
        fs.delete(new Path(subTaskPath[3] + "_temp"), true);
        // 获取job2_4_2中设置的计数器-其值为歌曲最多的流派的字符串在数组中的下标
        String maxGenreName = genres[(int) job2_4_2.getCounters().findCounter("genre", "max").getValue()];
        // 绘制词云图
        drawPng(fs,4 , new Path(subTaskPath[3] + "/task24/" + maxGenreName + ".txt") , new Path(subTaskPath[3] + "/task24/task24.png") );

        // Task25执行两个mapreduceJob,分别为GenreInformation GenreMining
        // job2_5_1 将通过track_id联系的Genre 各个属性改为Genre-属性键值对
        Job job2_5_1 = Job.getInstance(conf, "task2.GenreInformation");

        job2_5_1.setJarByClass(GenreMining.class);

        job2_5_1.setOutputKeyClass(Text.class);
        job2_5_1.setOutputValueClass(Text.class);

        job2_5_1.setMapperClass(GenreMining.GenreInformationMapper.class);
        job2_5_1.setReducerClass(GenreMining.GenreInformationReducer.class);

        job2_5_1.setInputFormatClass(TextInputFormat.class);
        job2_5_1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2_5_1, new Path(subInputPath[0]));
        FileInputFormat.addInputPath(job2_5_1, new Path(subInputPath[1]));
        FileOutputFormat.setOutputPath(job2_5_1, new Path(subTaskPath[4] + "_temp"));

        job2_5_1.waitForCompletion(true);

        src = new Path(subTaskPath[4] + "_temp/part-r-00000");

        // job2_5_2 根据job2_5_1得到的Genre-属性键值对，计算平均值绘制折线图
        Job job2_5_2 = Job.getInstance(conf, "task2.GenreMining");
        job2_5_2.setJarByClass(FrequencyStatistics.class);

        job2_5_2.setOutputKeyClass(Text.class);
        job2_5_2.setOutputValueClass(Text.class);

        job2_5_2.setMapperClass(GenreMining.GenreMiningMapper.class);
        job2_5_2.setReducerClass(GenreMining.GenreMiningReducer.class);

        job2_5_2.setInputFormatClass(TextInputFormat.class);
        job2_5_2.setOutputFormatClass(TextOutputFormat.class);
        // 输入为job2_5_1的输出
        FileInputFormat.addInputPath(job2_5_2, src);
        FileOutputFormat.setOutputPath(job2_5_2, new Path(subTaskPath[4]));

        job2_5_2.waitForCompletion(true);
        src = new Path(subTaskPath[4] + "/part-r-00000");
        dst = new Path(subTaskPath[4] + "/task25.txt");
        fs.rename(src, dst);

        drawPng(fs, 5 , dst, new Path(subTaskPath[4] + "/task25.png"));

        fs.delete(new Path(subTaskPath[4] + "_temp"), true);
    }

    private static void drawPng(FileSystem fs, int taskId, Path src, Path dst) throws Exception {
        FSDataInputStream in;
        FSDataOutputStream out;
        byte[] pngData = null;
        in = fs.open(src);
        switch (taskId) {
            case 3:
                pngData = DrawChart.drawTask3(in);
                break;
            case 4:
                pngData = DrawChart.drawTask4(in);
                break;
            case 5:
                pngData = DrawChart.drawTask5(in);
                break;
        }
        out = fs.create(dst);
        if (pngData != null) {
            out.write(pngData);
        }
        in.close();
        out.close();
    }
}
