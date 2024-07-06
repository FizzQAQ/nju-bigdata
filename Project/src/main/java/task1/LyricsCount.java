package task1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;


public class LyricsCount {
    public static class LyricsCountMapper extends Mapper<Object, Text, Text, Text> {
        private List<String> dict = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(conf.get("dictpath")));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.charAt(0) == '%') {
                    String[] temp = line.substring(1).split("[%,]");
                    for (String s : temp) {
                        dict.add(s);
                    }
                    break;
                }
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("songs.txt")) {
                String[] temp = value.toString().split("[ ,\t]");
                context.write(new Text(temp[1]), new Text("ISEXIST"));
                //System.out.println(key.toString()+","+"ISEXIST");
            } else {
                String[] temp = value.toString().split("[ ,]");
                if (temp[0].charAt(0) != '#' && temp[0].charAt(0) != '%') {
                    String track_id = temp[0];
                    int length = temp.length;
                    for (int i = 2; i < length; i++) {
                        String[] secondtemp = temp[i].split(":");
                        String word = dict.get(Integer.parseInt(secondtemp[0]) - 1);
                        word += ":" + secondtemp[1];
                        context.write(new Text(track_id), new Text(word));
                    }
                }
            }
        }
    }

    public static class LyricsCountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean exist = false;
            List<String> towrites = new ArrayList<>();
            for (Text value : values) {
                String isexist = value.toString();
                if (isexist.equals("ISEXIST")) {
                    exist = true;
                } else {
                    towrites.add(isexist);
                }
            }
            if (exist&&!towrites.isEmpty()) {
                String out = "[";
                for (String towrite : towrites) {
                    out += towrite + ",";
                }
                int i = out.length();
                StringBuilder sb = new StringBuilder(out);
                sb.replace(i - 1, i, String.valueOf(']'));
                context.write(key, new Text(sb.toString()));
            }
        }
    }
}
