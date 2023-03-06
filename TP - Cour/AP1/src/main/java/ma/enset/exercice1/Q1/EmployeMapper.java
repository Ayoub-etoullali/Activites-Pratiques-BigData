package ma.enset.exercice1.Q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmployeMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] text =value.toString().split(",");
        String department=text[2];
        double prix=Double.parseDouble(text[4]);
        context.write(new Text(department),new DoubleWritable(prix));
    }
}