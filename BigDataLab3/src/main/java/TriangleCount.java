import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class TriangleCount {

    public static class TriangleCountMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text("edge:" + line[1]));// 已经存在边的点
            if (line[1].contains(",")) {
                String[] str = line[1].split(",");
                for (int i = 0; i < str.length - 1; i++) {
                    for (int j = i + 1; j < str.length; j++) {
                        // 需要存在的边
                        context.write(new Text(str[i]), new Text(str[j]));// 若存在该边则存在一个三角形
                    }
                }
            }
        }
    }

    public static class TriangleCountReducer extends Reducer<Text, Text, Text, Text> {
        int num;

        public void reduce(Text key, Iterable<Text> values, Context context) {
            HashSet<String> set = new HashSet<>();
            ArrayList<String> list = new ArrayList<>();
            for (Text val : values) {
                String tmp = val.toString();
                if (tmp.startsWith("edge:")) {
                    tmp = tmp.substring(5);
                    String[] array = tmp.split(",");
                    set.addAll(Arrays.asList(array));
                } else {
                    list.add(tmp);
                }
            }
            for (String s : list) {
                if (set.contains(s)) {
                    num++;
                }
            }
        }

        public void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(String.valueOf(num)), new Text(""));
        }
    }
}
