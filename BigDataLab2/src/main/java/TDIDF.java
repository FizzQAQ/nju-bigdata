import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class TDIDF {    
    public static class TDIDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit)context.getInputSplit();
            String fileName = file.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString(),"\t\n\r\f,.:;?![]\"");
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                token=token.toLowerCase();
                if (map.containsKey(token)) {
                    map.put(token, map.get(token) + 1);
                } else {
                    map.put(token, 1);
                }
            }
            for (String token : map.keySet()) {
                context.write(new Text(token), new Text(fileName + ":" + map.get(token)));//某词，文件名，某词在该文件中出现的次数
            }
            
        }
    }
    public static class TDIDFReducer extends Reducer<Text, Text, Text, Text> {
        private int docCount;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            docCount = context.getConfiguration().getInt("docCount", 1);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> filewords = new HashMap<String, Integer>();            
   
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                String fileName = parts[0];
                int count = Integer.parseInt(parts[1]);
                if (filewords.containsKey(fileName)) {
                    filewords.put(fileName, filewords.get(fileName) + count);
                } else {
                    filewords.put(fileName, count);
                }
            }
            int containdoc=filewords.size();
            double idf = Math.log((double)docCount / containdoc);
            for (String fileName : filewords.keySet()) {
                double tfidf = filewords.get(fileName) * idf;
                context.write(new Text(fileName+","), new Text(key.toString() + "," + String.format("%.2f", tfidf)));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        int docCount = status.length;
        conf.setInt("docCount", docCount);
        Job job = Job.getInstance(conf, "TDIDF");
        job.setJarByClass(TDIDF.class);
        job.setMapperClass(TDIDFMapper.class);
        job.setReducerClass(TDIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
