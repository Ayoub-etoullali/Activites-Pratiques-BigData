package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class VenteDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job= Job.getInstance(new Configuration());
        //les classes Mapper et Reducer
        job.setMapperClass(VenteMapper.class);
        job.setReducerClass(VenteReducer.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        //les types de sotie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //le fomat input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/output
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setJarByClass(VenteDriver.class); //+
        job.waitForCompletion(true); // pour afficher les détails
    }
}
