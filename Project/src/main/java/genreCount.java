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
public class genreCount {
    public static class genreCountMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(" ,\t");
            if(temp[0]!="#"&&temp[0]!="%"){
                context.write(new Text(temp[0]),new Text(temp[1]));
            }
        }
    }
    public static class genreCountReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                String wordcount=value.toString();
                String out=",";
                out+=wordcount;
                context.write(key,new Text(out));
            }
        }
    }
}
