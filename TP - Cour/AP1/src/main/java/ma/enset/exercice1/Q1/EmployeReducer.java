package ma.enset.exercice1.Q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class EmployeReducer extends Reducer<Text, DoubleWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> iterator=values.iterator();
        double max=iterator.next().get();
        double min=max;
        double next;
        while (iterator.hasNext()){
            next=iterator.next().get();
            max= Math.max(max,next);
            min= Math.min(min,next);
        }
        String res="("+max+","+min+")";
        context.write(new Text(key+" (max,min) ="),new Text(res));
    }
}
