import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit)context.getInputSplit();
            String fileName = file.getPath().getName();
            Text fileNameText = new Text(fileName);
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                context.write(new Text(token), fileNameText);
            }
        }
    }
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
            Integer count = 0;
            for (Text value : values) {
                String fileName = value.toString();
                if (map.containsKey(fileName)) {
                    map.put(fileName, map.get(fileName) + 1);
                } else {
                    map.put(fileName, 1);
                }
                count++;
            }
            StringBuilder sb = new StringBuilder();
            String towrite=key.toString() + "\t";
            sb.append(String.format("%.2f", (double)count / map.size()) + ",");
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String fileName = iter.next();
                String count_in_file=map.get(fileName).toString();
                sb.append(fileName + ":" + count_in_file+";");
            }
            context.write(new Text(towrite), new Text(sb.toString()));
        }
    
        
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}