import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Job job=Job.getInstance(new Configuration());
        //les classes Mapper et Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //les types de sotie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //le fomat input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/output
        FileInputFormat.addInputPath(job,new Path(args[0])); // "/Tests/Test1/names.txt"
        FileOutputFormat.setOutputPath(job,new Path(args[1])); // "/Tests/Test1/output"

        job.setJarByClass(WordCountDriver.class); //+
        job.waitForCompletion(true); // pour afficher les détails

        /*
        hadoop jar TP_MAPREDUCE-1.0-SNAPSHOT.jar WordCountDriver /Tests/Test1/names.txt /Tests/Test1/output
         */
    }
}
