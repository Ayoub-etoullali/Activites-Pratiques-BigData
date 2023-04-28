package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class WriteText {
    public static void main(String[] args) throws IOException {

        Configuration conf=new Configuration();
//        conf.set("fs.defaultFS","hdfs://localhost:50070");
        conf.set("ACER/172.17.128.1","hdfs://localhost:50070");

        FileSystem fileSystem=FileSystem.get(conf);
//        Path path=new Path("/file.txt");
        Path path=new Path("C:\\Users\\pc\\Desktop\\ENSET\\S4\\Big data\\Tests\\HDFS\\src\\main\\resources\\input\\file.txt");

        FSDataOutputStream fsdos=fileSystem.create(path);
        BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fsdos, StandardCharsets.UTF_8));

        bw.write("ayoub");
        bw.newLine();
        bw.write("ETOULLALI");

        fsdos.close();
        fileSystem.close();
    }
}