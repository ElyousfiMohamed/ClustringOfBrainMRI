import entities.Pixel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<Integer> centers = new ArrayList<>();
    FileSystem fs;
    BufferedWriter bw;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        URI uri[] = context.getCacheFiles();
        try {
            fs = FileSystem.get(new URI("hdfs://localhost:9000/"), context.getConfiguration());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri[0]))));
        String ligne = "";
        while ((ligne = br.readLine()) != null) {
            centers.add(Integer.parseInt(ligne));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] x_y_c = value.toString().split(" ");
        Pixel p = new Pixel(Integer.parseInt(x_y_c[0]), Integer.parseInt(x_y_c[1]), Integer.parseInt(x_y_c[2]));
        int min = Integer.MAX_VALUE, d, nearestCenter = 0;
        for (Integer c : centers) {
            d = Math.abs(p.getColor() - c.intValue());
            if (d < min) {
                min = d;
                nearestCenter = c;
            }
        }

        FileWriter fw = new FileWriter("./output/" + nearestCenter + "_center.kmeans", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.append(p + "\n");
        bw.close();

        context.write(new Text(String.valueOf(nearestCenter)), new Text(p.toString()));
    }
}