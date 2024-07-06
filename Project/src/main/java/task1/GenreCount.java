package task1;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;


public class GenreCount {
    public static class GenreCountMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("songs.txt")) {
                String[] temp = value.toString().split("[ ,\t]");
                context.write(new Text(temp[1]), new Text("ISEXIST"));
                //System.out.println(key.toString()+","+"ISEXIST");
            } else {
                String[] temp = value.toString().split("[ ,\t]");
                if (temp[0].charAt(0) != '#' && temp[0].charAt(0) != '%') {
                    context.write(new Text(temp[0]), new Text(temp[1]));
                    //System.out.println(temp[0]+","+temp[1]);
                }
            }
        }
    }

    public static class GenreCountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean exist = false;
            List<String> towrites = new ArrayList<>();
            for (Text value : values) {
                String isexist = value.toString();
                //System.out.println(key.toString()+","+isexist);
                if (isexist.equals("ISEXIST")) {
                    exist = true;
                } else {
                    towrites.add(isexist);
                }
            }
            if (exist) {
                for (String towrite : towrites) {
                    context.write(key, new Text(towrite));
                }

            }
        }
    }
}
