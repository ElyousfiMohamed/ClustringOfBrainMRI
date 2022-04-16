import entities.Pixel;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class KmeansDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        boolean done = false;
        int x = 0;

        // create a file that contains the data about the pixels x,y,color(0 - 255)
        BufferedImage image = ImageIO.read(fs.open(new Path("hdfs://localhost:9000/images/brain_mri.jpg")));
        /*StringBuffer pixels = new StringBuffer();
        for (int i = 0; i < image.getWidth(); i++) {
            for (int j = 0; j < image.getHeight(); j++) {
                Color mycolor = new Color(image.getRGB(i,j));
                if (mycolor.getBlue() > 0)
                    pixels.append(i + " " + j + " " + mycolor.getBlue()+"\n");
            }
        }

        BufferedWriter bw1 = new BufferedWriter( new OutputStreamWriter(fs.create(new Path("hdfs://localhost:9000/input/pixels.txt"))) );
        bw1.write(pixels.toString());*/

        while (true) {
            Path f = new Path("hdfs://localhost:9000/outputTemp");
            if (fs.exists(f)) {
                fs.delete(f, true);
            }

            Job job = Job.getInstance(conf, "K_Means " + x++);
            job.setJarByClass(KmeansDriver.class);

            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.addCacheFile(new URI("hdfs://localhost:9000/input/center.txt"));

            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/pixels.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/outputTemp"));

            System.out.println("************************************");

            java.nio.file.Path path = Paths.get("./output");
            if (Files.exists(path))
                FileUtils.deleteDirectory(new File("./output"));

            Files.createDirectory(path);

            job.waitForCompletion(true);

            if (fs.exists(f)) {
                InputStreamReader is = new InputStreamReader(fs.open(new Path("hdfs://localhost:9000/outputTemp/part-r-00000")));
                BufferedReader br = new BufferedReader(is);

                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("hdfs://localhost:9000/input/center.txt"))));
                String line = "";
                while ((line = br.readLine()) != null) {
                    String[] centers = line.split("\\t");
                    int old = Integer.parseInt(centers[0]);
                    int ne_w = Integer.parseInt(centers[1]);
                    bw.append(String.valueOf(ne_w));
                    bw.newLine();
                    if (old == ne_w) {
                        done = true;
                    } else {
                        done = false;
                    }
                }

                if (done)
                    break;

                bw.close();
                br.close();
                fs.delete(f, true);
            }
        }

        try {
            String[] paths = {"cerebrospinal_fluid", "white_matter", "gray_matter"};
            int y = 0;
            File[] fileList = new File("./output").listFiles();
            for (File f0 : fileList) {
                BufferedImage bufferedImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
                Scanner obj = new Scanner(f0);
                while (obj.hasNextLine()) {
                    String[] x_y_c = obj.nextLine().split(" ");
                    Pixel p = new Pixel(Integer.parseInt(x_y_c[0]), Integer.parseInt(x_y_c[1]), Integer.parseInt(x_y_c[2]));
                    bufferedImage.setRGB(p.getX(), p.getY(), image.getRGB(p.getX(), p.getY()));
                }
                File f = new File("./output/" + paths[y]);
                ImageIO.write(bufferedImage, "jpg", f);
                fs.copyFromLocalFile(new Path("./output/" + paths[y++]), new Path("hdfs://localhost:9000/output/"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}