package com.razvan;

/**
 * Created by razvan.salajan on 4/11/17.
 */

        import com.razvan.MyInputFormat;
        import com.razvan.mapper.FindLinesMapper;
        import com.razvan.reducer.FindLinesReducer;
        import java.io.PrintStream;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Main.class);
        job.setJobName("Index Inversat");
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.addInputPath((Job)job, (Path)new Path(args[0]));
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(args[1]));
        job.setMapperClass(FindLinesMapper.class);
        job.setReducerClass(FindLinesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
