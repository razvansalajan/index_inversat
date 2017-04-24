package com.razvan.mapper;

/**
 * Created by razvan.salajan on 4/11/17.
 */


import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FindLinesMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    private Set<String> mySet;
    private boolean local = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException  {
        this.mySet = new HashSet<String>();
        if (!this.local) {
            Path pt = new Path("hdfs://hadoop1/user/hduser/stopwords.txt");
            FileSystem fs = FileSystem.get((Configuration) new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader((InputStream) fs.open(pt)));
            String line = br.readLine();
            while (line != null) {
                this.mySet.add(line);
                line = br.readLine();
            }
        }
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words;
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        words = value.toString().split("[^a-zA-Z]+");
        for (String word : words) {
            if (word.length() == 0 || this.mySet.contains(word)) continue;
            context.write(new Text(word), new Text(fileName + " " + key.toString()));
        }
        System.out.println(fileName + " " + key + " " + value);
    }
}
