import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.print.DocFlavor.STRING;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class LyricsCount {
    public static class LyricsCountMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(" ,");
            if(temp[0]!="#"&&temp[0]!="%"){
                String track_id=temp[0];
                int length=temp.length;
                for(int i=2;i<length;i++){
                    context.write(Text(track_id),new Text(temp[i]));
                }
            }
        }
    }
    public static class LyricsCountReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                String wordcount=value.toString();
                String out=",[";
                out+=wordcount+",";
            }
            out[out.length()-1]="]";
            context.write(key,new Text(out));
        }
    }
}
