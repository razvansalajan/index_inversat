

/**
 * Created by razvan.salajan on 4/11/17.
 */
package com.razvan;

import com.razvan.MyRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

/*
 * This class specifies class file version 49.0 but uses Java 6 signatures.  Assumed Java 6.
 */
public class MyInputFormat
        extends NLineInputFormat {

//    @Override
//    protected boolean isSplitable(JobContext context, Path file) {
//        return false;
//    }
//

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        ArrayList splits = new ArrayList();
//        int numLinesPerSplit = getNumLinesPerSplit(job);
        Iterator i$ = this.listStatus(job).iterator();

        while (i$.hasNext()) {
            FileStatus status = (FileStatus) i$.next();
            splits.addAll(myGetSplitsForFile(status, job.getConfiguration()));
        }

        return splits;
    }


    public static List<FileSplit> myGetSplitsForFile(FileStatus status, Configuration conf) throws IOException {
        ArrayList splits = new ArrayList();
        Path fileName = status.getPath();
        Integer numLinesPerSplit = 100;
        if(status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        } else {
            FileSystem fs = fileName.getFileSystem(conf);
            LineReader lr = null;

            try {
                FSDataInputStream in = fs.open(fileName);
                lr = new LineReader(in, conf);
                Text line = new Text();
                int numLines = 0;
                long begin = 0L;
                long length = 0L;
                boolean num = true;
                int idxLine = 0;
                int var18;
                while((var18 = lr.readLine(line)) > 0) {
                    ++numLines;
                    ++idxLine;
                    length += (long)var18;
                    if(numLines == numLinesPerSplit) {
                        splits.add(createFileSplit(idxLine, fileName, begin, length));
                        begin += length;
                        length = 0L;
                        numLines = 0;
                    }
                }

                if(numLines != 0) {
                    splits.add(createFileSplit(idxLine, fileName, begin, length));
                }
            } finally {
                if(lr != null) {
                    lr.close();
                }

            }

            return splits;
        }
    }

    protected static MyInputSplit createFileSplit(Integer idxLine, Path fileName, long begin, long length) {
        return begin == 0L?new MyInputSplit(idxLine, fileName, begin, length - 1L, new String[0]):new MyInputSplit(idxLine, fileName, begin - 1L, length, new String[0]);
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        MyRecordReader myRecordReader = new MyRecordReader();
        try {
            myRecordReader.initialize(split, context);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return myRecordReader;
    }
}
