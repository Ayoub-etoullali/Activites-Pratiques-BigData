package ma.enset.exercice2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] text = value.toString().split(",");
        String[] date = text[1].split("-");
        String annee = date[0].substring(1,5);
        System.out.println(annee);
        int tmp = Integer.parseInt(text[14]);
        System.out.println(tmp);
        context.write(new Text(annee), new IntWritable(tmp));
    }
}