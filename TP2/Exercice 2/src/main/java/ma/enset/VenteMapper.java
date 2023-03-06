package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VenteMapper extends Mapper<LongWritable, Text, Text, FloatWritable> { //ligneµ

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        String text[]=value.toString().split(" ");
        String ville=text[1];
        String annee=text[0].split("/")[2];
        float prix=Float.parseFloat(text[3]);
        context.write(new Text(ville+" "+annee),new FloatWritable(prix));
    }
}