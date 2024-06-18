import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;


public class Kmeans {
    
    public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private List<double[]> clusters = new ArrayList<double[]>();
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(conf.get("centroidsPath")));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                String[] temp = line.split(",: ");
                double[] cluster = new double[temp.length-1];
                for (int i = 0; i < temp.length-1; i++) {
                    cluster[i] = Double.parseDouble(temp[i+1]);
                }
                clusters.add(cluster);
            }
            br.close();
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",: ");
            double[] data = new double[temp.length-1];
            for (int i = 0; i < temp.length-1; i++) {
                data[i] = Double.parseDouble(temp[i+1]);
            }
            double minDistance = Double.MAX_VALUE;
            int clusterIndex = -1;
            for (int i = 0; i < clusters.size(); i++) {
                double distance = 0;
                for (int j = 0; j < data.length; j++) {
                    distance += Math.pow(data[j] - clusters.get(i)[j], 2);
                }
                if (distance < minDistance) {
                    minDistance = distance;
                    clusterIndex = i;
                }
            }
            System.out.println("map-key :" + clusterIndex + "map-value: " + value.toString());
            context.write(new IntWritable(clusterIndex), new Text(value.toString()));
        }
    }
    
    public static class KmeansReducer extends Reducer<IntWritable,Text, IntWritable, Text> {
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> dataList = new ArrayList<double[]>();
            for (Text value : values) {
                String[] temp = value.toString().split(",: ");
                double[] data = new double[temp.length-1];
                for (int i = 0; i < temp.length-1; i++) {
                    data[i] = Double.parseDouble(temp[i+1]);
                }
                dataList.add(data);
            }
            double[] newCluster = new double[dataList.get(0).length];
            for (int i = 0; i < dataList.get(0).length; i++) {
                double sum = 0;
                for (int j = 0; j < dataList.size(); j++) {
                    sum += dataList.get(j)[i];
                }
                newCluster[i] = sum / dataList.size();
            }
            String out = ": ";
            for (int i = 0; i < newCluster.length; i++) {
                out += newCluster[i];
                if(i != newCluster.length-1) {
                    out += ", ";
                }
            }
            System.out.println("reduce-result:" + " key:" + key + " out: " + out);
            context.write(key, new Text(out));
        }
    }
}
