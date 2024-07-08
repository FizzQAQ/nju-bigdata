import org.apache.hadoop.conf.Configuration;
import task1.Task1Driver;
import task2.Task2Driver;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws Exception {
        // 输入格式如下，可以只输入一个参数，即数据集路径，三部分任务有默认输出路径即task1 task2 task3,也可以自己输入
        // jar请使用jar-with-dependencies.jar
        // jar ****.jar path/to/dataSet
        // jar ****.jar path/to/dataSet path/to/task1/output
        // jar ****.jar path/to/dataSet path/to/task1/output path/to/task2/output
        // jar ****.jar path/to/dataSet path/to/task1/output path/to/task2/output path/to/task3/output
        //configuration
        Configuration conf = new Configuration();
        Task1Driver.driver(conf,args);
        Task2Driver.driver(conf,args);
    }
}