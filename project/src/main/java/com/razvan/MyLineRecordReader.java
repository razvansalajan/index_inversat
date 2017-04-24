package com.razvan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;

import java.io.IOException;

/**
 * Created by razvan.salajan on 4/25/17.
 */
public class MyLineRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(MyLineRecordReader.class);
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes = null;

    public MyLineRecordReader() {
    }

    public MyLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        MyInputSplit split = (MyInputSplit)genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        this.codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if(this.isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if(this.codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)this.codec).createInputStream(fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new SplitLineReader(this.codec.createInputStream(fileIn, this.decompressor), job, this.recordDelimiterBytes);
                this.filePosition = fileIn;
            }
        } else {
            fileIn.seek(this.start);
            this.in = new UncompressedSplitLineReader(fileIn, job, this.recordDelimiterBytes, split.getLength());
            this.filePosition = fileIn;
        }

        if(this.start != 0L) {
            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
        }

        this.pos = this.start;
    }

    private boolean isCompressedInput() {
        return this.codec != null;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput()?2147483647:(int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if(this.isCompressedInput() && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        int newMaxLineLength = (int)Math.min(3L + (long)this.maxLineLength, 2147483647L);
        int newSize = this.in.readLine(this.value, newMaxLineLength, this.maxBytesToConsume(this.pos));
        this.pos += (long)newSize;
        int textLength = this.value.getLength();
        byte[] textBytes = this.value.getBytes();
        if(textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if(textLength > 0) {
                textBytes = this.value.copyBytes();
                this.value.set(textBytes, 3, textLength);
            } else {
                this.value.clear();
            }
        }

        return newSize;
    }

    public boolean nextKeyValue() throws IOException {
        if(this.key == null) {
            this.key = new LongWritable();
        }

        this.key.set(this.pos);
        if(this.value == null) {
            this.value = new Text();
        }

        int newSize = 0;

        while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
            if(this.pos == 0L) {
                newSize = this.skipUtfByteOrderMark();
            } else {
                newSize = this.in.readLine(this.value, this.maxLineLength, this.maxBytesToConsume(this.pos));
                this.pos += (long)newSize;
            }

            if(newSize == 0 || newSize < this.maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        if(newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    public LongWritable getCurrentKey() {
        return this.key;
    }

    public Text getCurrentValue() {
        return this.value;
    }

    public float getProgress() {
        if(this.start == this.end) {
            return 0.0F;
        } else {
            try {
                return Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
            } catch (IOException var2) {
                throw new RuntimeException(var2);
            }
        }
    }

    public synchronized void close() throws IOException {
        try {
            if(this.in != null) {
                this.in.close();
            }
        } finally {
            if(this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
                this.decompressor = null;
            }

        }

    }
}

