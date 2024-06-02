import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.util.TreeSet;


public class EdgeList {

    public static class EdgeListMapper extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String temp = value.toString().split("\t")[0];
            String[] websiteId = temp.split(" ");
            context.write(new Text(websiteId[0]),new Text(websiteId[1]));
        }
    }

    public static class EdgeListReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeSet<String> treeSet = new TreeSet<String>();
            for (Text val : values) {
                treeSet.add(val.toString());
            }

            String list = "";
            Iterator<String> it = treeSet.iterator();
           while(it.hasNext()){
               list += it.next();
                if(it.hasNext())
                    list += ",";
            }
            context.write(key, new Text(list));
        }
    }
}
