package ma.enset.exercice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DataReducer extends Reducer<Text, IntWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator=values.iterator();
        int max=iterator.next().get();
        int min=max;
        int next;
        while (iterator.hasNext()){
            next=iterator.next().get();
            max= Math.max(max,next);
            min= Math.min(min,next);
        }
        String res="("+max+","+min+")";
        context.write(new Text(key+" (max,min) ="),new Text(res));
    }
}
