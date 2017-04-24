package com.razvan.reducer;

/**
 * Created by razvan.salajan on 4/11/17.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * This class specifies class file version 49.0 but uses Java 6 signatures.  Assumed Java 6.
 */
public class FindLinesReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Set<String>> myMap = new HashMap<String, Set<String>>();
        for (Text text : values) {
            String[] words = text.toString().split(" ");
            String fileName = words[0];
            String lineIndex = words[1];
            if (!myMap.containsKey(fileName)) {
                Set<String> mySet = new HashSet<String>();
                mySet.add(lineIndex);
                myMap.put(fileName, mySet);
                continue;
            }
            myMap.get(fileName).add(lineIndex);
        }
        String toWrite = "";
        for (Map.Entry<String, Set<String>> currEntry : myMap.entrySet()) {
            String currLine = "(" + currEntry.getKey();
            Set<String> mySet = currEntry.getValue();
            for (String s : mySet) {
                currLine = currLine + ", " + s;
            }
            currLine = currLine + ")";
            toWrite = toWrite + currLine + ";";
        }
        context.write(new Text(key), new Text(toWrite));
    }
}
