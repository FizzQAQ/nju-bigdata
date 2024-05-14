import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF {
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            double totalWords = 0;
            HashMap<String, Double> hashMap = new HashMap<String, Double>();
            Pattern pattern = Pattern.compile("\\w+(-\\w+)*('\\w+)?", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                String word = matcher.group();
                word = word.toLowerCase();
                if (hashMap.containsKey(word)) {
                    hashMap.put(word, hashMap.get(word) + 1.0);
                } else {
                    hashMap.put(word, 1.0);
                }
                totalWords += 1.0;
            }
            // StringTokenizer itr = new StringTokenizer(value.toString(),
            // "\t\n\r\f,.:;?![]\"\40-()'");
            // while (itr.hasMoreTokens()) {
            // String token = itr.nextToken();
            // token = token.toLowerCase();
            // if (hashMap.containsKey(token)) {
            // hashMap.put(token, hashMap.get(token) + 1.0);
            // } else {
            // hashMap.put(token, 1.0);
            // }
            // totalWords += 1.0;
            // }
            for (String token : hashMap.keySet()) {
                // key: 词名，value: (文件名，TF)
                context.write(new Text(token), new Text(fileName + " " + hashMap.get(token) / totalWords));
            }

        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private int docCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            docCount = context.getConfiguration().getInt("docCount", 1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> filewords = new HashMap<String, Double>();

            for (Text value : values) {
                String[] parts = value.toString().split(" ");
                String fileName = parts[0];
                double tf = Double.parseDouble(parts[1]);
                if (filewords.containsKey(fileName)) {
                    filewords.put(fileName, filewords.get(fileName) + tf);
                } else {
                    filewords.put(fileName, tf);
                }
            }
            int containdoc = filewords.size() + 1;
            double idf = Math.log((double) docCount / containdoc);
            for (String fileName : filewords.keySet()) {
                double tfidf = filewords.get(fileName) * idf;
                context.write(new Text(fileName + ","), new Text(key.toString() + "," + String.format("%.2f", tfidf)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        int docCount = status.length;
        conf.setInt("docCount", docCount);
        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
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
