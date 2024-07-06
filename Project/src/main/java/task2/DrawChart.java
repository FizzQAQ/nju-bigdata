package task2;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.plot.PlotOrientation;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Scanner;

public class DrawChart {

    public static byte[] draw1(DataInputStream inputStream) throws IOException, Exception {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        try (Scanner scanner = new Scanner(inputStream)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] parts = line.split("\\),");
                dataset.addValue(Integer.parseInt(parts[1]),"Cnt", parts[0] + ")" );
            }
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "DurationStatistics", // 图表标题
                "Duration", // X轴标签
                "Count", // Y轴标签
                dataset, // 数据集
                PlotOrientation.VERTICAL, // 图表方向
                true, // 是否显示图例
                true, // 是否生成工具提示
                false // 是否生成URL链接
        );

        // 生成BufferedImage
        BufferedImage chartImage = chart.createBufferedImage(1200, 900);

        // 转化为byte[]输出
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(chartImage, "png", os);
        return os.toByteArray();
    }

    public static byte[] draw2(DataInputStream inputStream) throws IOException, Exception {

        return null;
    }

}