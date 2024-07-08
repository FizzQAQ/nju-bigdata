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
    // 绘制直方图
    public static byte[] drawTask3(DataInputStream inputStream) throws IOException, Exception {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        // 流操作将输入文件分割输入
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .map(line -> line.split("\\),"))
                    .forEach(parts -> dataset.addValue(Integer.parseInt(parts[1].trim()), "Cnt", parts[0] + ")"));
        }
        // 创建直方图
        JFreeChart chart = ChartFactory.createBarChart("DurationStatistics", // 图表标题
                "Duration",
                "Count",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // 生成BufferedImage
        BufferedImage chartImage = chart.createBufferedImage(1200, 900);

        // 转化为byte[]输出
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(chartImage, "png", os);
        return os.toByteArray();
    }

    // 绘制歌词词云图
    public static byte[] drawTask4(DataInputStream inputStream) throws IOException, Exception {
        List<WordFrequency> wordFrequencyList;
        // 流操作将输入文件分割输入
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            wordFrequencyList = reader.lines()
                    .map(line -> line.split(","))
                    .map(parts -> new WordFrequency(parts[0], Integer.parseInt(parts[1]))).collect(Collectors.toList());
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


        // 生成词云图 转化为byte[]输出
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        wordCloud.build(wordFrequencyList);
        wordCloud.writeToStream("png", os);
        return os.toByteArray();
    }

    // 绘制折线图
    public static byte[] drawTask5(DataInputStream inputStream) throws IOException, Exception {
        // 定义折线图各列属性字符串
        String[] column = {"energy", "tempo", "loudness", "duration", "danceability"};

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        // 流操作将输入文件分割输入
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