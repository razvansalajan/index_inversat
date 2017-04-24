package com.razvan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by razvan.salajan on 4/24/17.
 */
public class MyInputSplit extends InputSplit  implements Writable {
    private Path file;
    private long start;
    private long length;
    private String[] hosts;



    public Path getPath() {
        return this.file;
    }

    public long getStart() {
        return this.start;
    }

    public long getLength() {
        return this.length;
    }

    public String toString() {
        return this.file + ":" + this.start + "+" + this.length;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.file.toString());
        out.writeLong(this.start);
        out.writeLong(this.length);
        out.writeInt(this.index);
    }

    public void readFields(DataInput in) throws IOException {
        this.file = new Path(Text.readString(in));
        this.start = in.readLong();
        this.length = in.readLong();
        this.index = in.readInt();
        this.hosts = null;
    }

    public String[] getLocations() throws IOException {
        return this.hosts == null?new String[0]:this.hosts;
    }

    MyInputSplit(){
//        index =
    }
    public Integer index;

    public MyInputSplit(Integer index, Path file, long start, long length, String[] hosts) {
//        super(file, start, length, hosts);
        this.index = index;
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;

    }


}
