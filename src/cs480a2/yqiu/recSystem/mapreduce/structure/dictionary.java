package cs480a2.yqiu.recSystem.mapreduce.structure;

import java.util.Hashtable;

/**
 * Created by Qiu on 3/26/2015.
 * Data structure that stores all words occurs in a document associated with its TF-IDF value.
 * Key is the word, Value is its TF-IDF value
 */

public class dictionary extends Hashtable<String, Double> {

    @Override
    public synchronized Double put(String key, Double value) {
        return super.put(key, value);
    }

    /**
     * @param key Word
     * @return  TF-IDF value for a given word, 0 if the word does not exists.
     */
    @Override
    public synchronized Double get(Object key) {
        if (!this.containsKey(key)) {
            return 0.0;
        }
        return super.get(key);
    }
}
