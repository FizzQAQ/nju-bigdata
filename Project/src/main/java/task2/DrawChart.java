package task2;

import com.kennycason.kumo.wordstart.RandomWordStart;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.plot.PlotOrientation;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.palette.ColorPalette;
import com.kennycason.kumo.wordstart.CenterWordStart;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class DrawChart {

    public static byte[] drawTask3(DataInputStream inputStream) throws IOException, Exception {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .map(line -> line.split("\\),"))
                    .forEach(parts -> dataset.addValue(Integer.parseInt(parts[1].trim()), "Cnt", parts[0] + ")"));
        }

        JFreeChart chart = ChartFactory.createBarChart("DurationStatistics", // 图表标题
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

    public static byte[] drawTask4(DataInputStream inputStream) throws IOException, Exception {

        List<WordFrequency> wordFrequencyList;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            wordFrequencyList = reader.lines().map(line -> line.split(",")).map(parts -> new WordFrequency(parts[0], Integer.parseInt(parts[1]))).collect(Collectors.toList());
        }

        // 创建词云图配置
        Dimension dimension = new Dimension(600, 600);
        WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new RectangleBackground(dimension));
        wordCloud.setBackgroundColor(Color.WHITE);
        wordCloud.setColorPalette(new ColorPalette(Color.RED, Color.GREEN, Color.BLACK, Color.DARK_GRAY, Color.BLUE));
        wordCloud.setFontScalar(new LinearFontScalar(15, 100));
        wordCloud.setWordStartStrategy(new RandomWordStart());

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        // 生成词云图
        wordCloud.build(wordFrequencyList);
        wordCloud.writeToStream("png", os);
        return os.toByteArray();
    }

    public static byte[] drawTask5(DataInputStream inputStream) throws IOException, Exception {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        String[] column = {"energy", "tempo", "loudness", "duration", "danceability"};
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .map(line -> line.split(","))
                    .forEach(parts -> {
                        String genre = parts[0];
                        for (int i = 1; i < parts.length; i++) {
                            dataset.addValue(Double.parseDouble(parts[i]), genre, column[i - 1]);
                        }
                    });
        }

        // 创建折线图
        JFreeChart chart = ChartFactory.createLineChart(
                "GenreMining",   // 图表标题
                "Attribute",            // X 轴标签
                "Average",               // Y 轴标签
                dataset);              // 数据集

        // 生成BufferedImage
        BufferedImage chartImage = chart.createBufferedImage(1200, 900);

        // 转化为byte[]输出
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(chartImage, "png", os);
        return os.toByteArray();
    }

}