import org.apache.hadoop.conf.Configuration;
import task1.Task1Driver;
import task2.Task2Driver;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws Exception {
        // jar ****.jar 0path/to/dataSet
        // jar ****.jar 0path/to/dataSet path/to/task1/output
        // jar ****.jar 0path/to/dataSet path/to/task1/output path/to/task2/output
        // jar ****.jar 0path/to/dataSet path/to/task1/output path/to/task2/output path/to/task3/output
        //configuration
        Configuration conf = new Configuration();
        Task1Driver.driver(conf,args);
        Task2Driver.driver(conf,args);
    }
}