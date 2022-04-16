import entities.Pixel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class KmeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int somme = 0;
        int nb_points = 0, center;
        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            String[] x_y_color = it.next().toString().split(" ");
            Pixel p = new Pixel(Integer.parseInt(x_y_color[0]), Integer.parseInt(x_y_color[1]), Integer.parseInt(x_y_color[2]));
            somme += p.getColor();
            nb_points++;
        }

        center = somme / nb_points;
        context.write(key, new Text(String.valueOf(center)));
    }
}