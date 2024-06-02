import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws Exception {
        // Job1

        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "Edge Process");
        job1.setJarByClass(EdgeProcess.class);
        job1.setInputFormatClass(TextInputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(EdgeProcess.GetEdgeMapper.class);
        job1.setReducerClass(EdgeProcess.GetEdgeReducer.class);

        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/output1"));

        job1.waitForCompletion(true);

        //job2

        Job job2 = new Job(conf, "Edge Process");
        job2.setJarByClass(EdgeProcess.class);
        job2.setInputFormatClass(TextInputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(EdgeProcess.GetEdgeMapper.class);
        job2.setReducerClass(EdgeProcess.GetEdgeReducer.class);

        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job1, new Path(args[1] + "/output1"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/output2"));
        job2.waitForCompletion(true);

        //job3

        Job job3 = new Job(conf, "Edge Process");
        job3.setJarByClass(EdgeProcess.class);
        job3.setInputFormatClass(TextInputFormat.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(EdgeProcess.GetEdgeMapper.class);
        job3.setReducerClass(EdgeProcess.GetEdgeReducer.class);

        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]+"/output2"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/output3"));

        job3.waitForCompletion(true);


    }
}
