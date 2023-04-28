package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ReadText {
    public static void main(String[] args) throws IOException {

        Configuration conf=new Configuration();
        conf.set("ACER/172.17.128.1","hdfs://localhost:50070");
//        conf.set("fs.defaultFS","hdfs://localhost:9000");
        FileSystem fileSystem=FileSystem.get(conf);
        Path path=new Path("C:\\Users\\pc\\Desktop\\ENSET\\S4\\Big data\\Tests\\HDFS\\src\\main\\resources\\input\\file.txt");
//        Path path=new Path("/file.txt");
        FSDataInputStream fsdis=fileSystem.open(path);
        BufferedReader br=new BufferedReader(new InputStreamReader(fsdis, StandardCharsets.UTF_8));
        String ligne=null;
        while ((ligne=br.readLine())!=null){
            System.out.println(ligne);
        }
        fsdis.close();
        fileSystem.close();
    }
}