package cs480a2.yqiu.recSystem.mapreduce.structure;

import org.apache.hadoop.io.Text;

import java.util.HashMap;

/**
 * Created by Qiu on 04/15/2015.
 * Custom Hashmap for storing word and its count
 * When adding an existing key, instead of replacing the old one with new one
 * It add the new value to the old value.
 * This is useful to count word
 */

public class WordMap extends HashMap<Text, Integer> {

    @Override
    public Integer put(Text key, Integer value) {

        if (this.containsKey(key)) {//if key exists

            int oldVal = this.get(key);

            value = value + oldVal;
            super.put(key, value);

        } else {
            super.put(key, value);
        }
        return value;
    }

}

