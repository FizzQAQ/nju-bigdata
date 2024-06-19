import org.apache.hadoop.fs.FileSystem;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String centroidsPath = args[1];
        String outputPath = args[2];
        String tempOutputPath = outputPath + "_temp";
        String tempCenterPath = "center_temp/center.data";
        FileSystem fs = FileSystem.get(conf);
        FileUtil.copy(fs, new Path(centroidsPath), fs, new Path(tempCenterPath),false,conf);
        conf.set("centroidsPath", tempCenterPath);
        centroidsPath = tempCenterPath;
        boolean converged = false;
        while (!converged) {
            Job job = Job.getInstance(conf, "K-Means Clustering");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(Kmeans.KmeansMapper.class);
            job.setReducerClass(Kmeans.KmeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(tempOutputPath));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            // 比较新的簇中心和旧的簇中心
            List<double[]> oldCentroids = KmeansUtils.readCentroids(centroidsPath);
            List<double[]> newCentroids = KmeansUtils.readCentroids(tempOutputPath + "/part-r-00000");

            double maxDelta = KmeansUtils.calculateMaxDelta(oldCentroids, newCentroids);
            if (maxDelta == 0) {
                converged = true;
            } else {

                fs.delete(new Path(centroidsPath), true);
                fs.rename(new Path(tempOutputPath + "/part-r-00000"), new Path(centroidsPath));
                fs.delete(new Path(tempOutputPath), true);
            }
        }
        fs.rename(new Path(tempOutputPath), new Path(outputPath));
    }
}
