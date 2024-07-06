package task1;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;

public class UserCount {
    public static class UserCountMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit) context.getInputSplit();
            String fileName = file.getPath().getName();
            if (fileName.equals("songs.txt")) {
                String[] temp = value.toString().split("[ ,\t]");
                context.write(new Text(temp[0]), new Text("ISEXIST"));
                //System.out.println(key.toString()+","+"ISEXIST");
            } else {
                String[] temp = value.toString().split("[ ,\t]");
                if (temp[0].charAt(0) != '#' && temp[0].charAt(0) != '%') {
                    context.write(new Text(temp[1]), new Text(temp[0] + "," + temp[2]));
                }
            }
        }
    }

    public static class UserCountReducer extends Reducer<Text, Text, Text, Text> {
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
                for (String towrite : towrites) {
                    String[] tmp=towrite.split(",");
                    String userid=tmp[0];
                    String count=tmp[1];
                    String songid=key.toString();
                    context.write(new Text(userid),new Text(songid+","+count));
                }
            }
        }
    }


}
