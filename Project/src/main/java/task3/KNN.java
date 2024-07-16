package task3;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

public class KNN {

    public static class KNNMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

        Map<String, String> data = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Get pretrained data
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(conf.get("pretrainedData")));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String dataLine = null;
            while ((dataLine = bufferedReader.readLine()) != null) {
                data.put(dataLine.split(",")[0], dataLine.split(",")[1]);
            }
            bufferedReader.close();
            FSDataInputStream inputStream2 = fs.open(new Path(conf.get("lyrics")));
            BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(inputStream2));
            String dataLine2 = null;
            while ((dataLine2 = bufferedReader2.readLine()) != null) {
                String[] pair = dataLine2.split(";");
                String id = pair[0];
                if (data.containsKey(id)) {
                    data.put(id, data.get(id) + ";" + pair[1].substring(0, pair[1].length() - 1));
                }
            }
            bufferedReader2.close();
            List<String> keysToRemove = new ArrayList<>();
            for (String key : data.keySet()) {
                if (!(data.get(key).contains(";"))) {
                    keysToRemove.add(key);
                }
            }
            for (String key : keysToRemove) {
                data.remove(key);
            }
        }

        // data format:
        // key: <track id>
        // value: <emotion>;<word>:<count>,<word>:<count>,...

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(";");
            String trackID = tokens[0];
            String lyrics = tokens[1].substring(0, tokens[1].length() - 1);
            if (data.containsKey(trackID)) {
                context.write(new Text(trackID), new Text(data.get(trackID).split(";")[0]));
            } else {
                TreeMap<Double, String> distances = new TreeMap<>();
                int k = 5;
                for (String id : data.keySet()) {
                    double distance = euclideanDistance(lyrics.split(","), data.get(id).split(";")[1].split(","));
                    distances.put(distance, data.get(id).split(";")[0]);
                    if (distances.size() > k) {
                        distances.remove(distances.lastKey());
                    }
                }
                String n1 = "", n2 = "", n3 = "";
                double a1 = 0, a2 = 0, a3 = 0;
                for (double distance : distances.keySet()) {
                    // context.write(new Text(String.valueOf(distance)), new
                    // Text(distances.get(distance)));
                    if (n1 == "") {
                        n1 = distances.get(distance);
                        a1 += 1 / distance;
                    } else if (distances.get(distance).equals(n1)) {
                        a1 += 1 / distance;
                    } else if (n2 == "") {
                        n2 = distances.get(distance);
                        a2 += 1 / distance;
                    } else if (distances.get(distance).equals(n2)) {
                        a2 += 1 / distance;
                    } else if (n3 == "") {
                        n3 = distances.get(distance);
                        a3 += 1 / distance;
                    } else if (distances.get(distance).equals(n3)) {
                        a3 += 1 / distance;
                    }
                }
                // String tst = n1 + " " + a1 + " " + n2 + " " + a2 + " " + n3 + " " + a3;
                if (a1 >= a2 && a1 >= a3) {
                    context.write(new Text(trackID), new Text(n1));
                } else if (a2 >= a1 && a2 >= a3) {
                    context.write(new Text(trackID), new Text(n2));
                } else {
                    context.write(new Text(trackID), new Text(n3));
                }
            }
        }

        private double euclideanDistance(String[] a, String[] b) {
            Map<String, Double> map1 = new HashMap<>();
            Map<String, Double> map2 = new HashMap<>();
            for (String s : a) {
                String[] pair = s.split(":");
                map1.put(pair[0], Double.parseDouble(pair[1]));
            }
            double sum = 0;
            for (String s : b) {
                String[] pair = s.split(":");
                if (map1.containsKey(pair[0])) {
                    sum += Math.pow(Double.parseDouble(pair[1]) - map1.get(pair[0]), 2);
                } else {
                    sum += Math.pow(Double.parseDouble(pair[1]), 2);
                }
                map2.put(pair[0], Double.parseDouble(pair[1]));
            }
            for (String s : a) {
                String[] pair = s.split(":");
                if (!map2.containsKey(pair[0])) {
                    sum += Math.pow(Double.parseDouble(pair[1]), 2);
                }
            }
            return Math.sqrt(sum);
        }
    }
}
