package com.razvan;

/**
 * Created by razvan.salajan on 4/11/17.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader
        extends LineRecordReader {
    private long lineIndex = 0;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        MyInputSplit myInputSplit = (MyInputSplit)genericSplit;
        super.initialize(genericSplit, context);
        this.lineIndex = myInputSplit.index;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        boolean ans = super.nextKeyValue();
        if (!ans) {
            this.lineIndex = -1;
        }
        ++this.lineIndex;
        return ans;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(this.lineIndex);
    }
}
