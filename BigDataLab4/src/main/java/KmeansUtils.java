import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class KmeansUtils {

    public static List<double[]> readCentroids(String path) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream inputStream = fs.open(new Path(path));
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.replace(" ", "").split("[,:]");
            double[] centroid = new double[parts.length-1];
            for (int i = 0; i < parts.length-1; i++) {
                centroid[i] = Double.parseDouble(parts[i+1]);
            }
            centroids.add(centroid);
        }
        br.close();
        return centroids;
    }
    public static double calculateMaxDelta(List<double[]> oldCentroids, List<double[]> newCentroids) {
        double maxDelta = 0;
        for (int i = 0; i < oldCentroids.size(); i++) {
            double delta = 0;
            for (int j = 0; j < oldCentroids.get(i).length; j++) {
                delta += Math.pow(oldCentroids.get(i)[j] - newCentroids.get(i)[j], 2);
            }
            if (delta > maxDelta) {
                maxDelta = delta;
            }
        }
        return maxDelta;
    }
}
