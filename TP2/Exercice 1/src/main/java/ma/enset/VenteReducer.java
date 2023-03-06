package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class VenteReducer extends Reducer<Text, FloatWritable,Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> iterator=values.iterator();
        double somme=0;
        while (iterator.hasNext()){
            somme+=iterator.next().get();
        }
        context.write(new Text(key),new DoubleWritable(somme));
        /*
        fichier caché : context.getCacheFiles()
         */
    }
}
