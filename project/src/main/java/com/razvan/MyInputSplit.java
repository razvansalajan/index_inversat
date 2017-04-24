package com.razvan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Created by razvan.salajan on 4/24/17.
 */
public class MyInputSplit extends FileSplit {

    public Integer index = 0;
    public MyInputSplit(Integer index, Path file, long start, long length, String[] hosts) {
        super(file, start, length, hosts);
        this.index = index;
    }


}
