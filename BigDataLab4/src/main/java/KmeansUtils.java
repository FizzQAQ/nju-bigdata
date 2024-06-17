import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KmeansUtils {

    public static List<double[]> readCentroids(String path) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            double[] centroid = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                centroid[i] = Double.parseDouble(parts[i]);
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
