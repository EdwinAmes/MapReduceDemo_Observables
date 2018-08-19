package com.ps.mapreducedemo.reduce;

import com.ps.mapreducedemo.MapReduceProcessor;
import com.ps.mapreducedemo.util.IoUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.List;

/**
 * Sums up word count over all files.
 */
public class WordCountCalculator extends MapReduceProcessor {
    static Logger logger = LogManager.getLogger(WordCountCalculator.class);

    public WordCountCalculator(IoUtils ioUtils) {
        super(ioUtils);
    }

    /**
     * Read all partition file for specified word. Add up the word counts.
     * @param word
     * @param inputPath
     */
    public long sumWordCounts(String word, Path inputPath) {
        long totalCountForWord = 0;
        List<Path> partitionFilePathList = ioUtils.getSubPaths(inputPath, word + ".*.mp");
        for(Path partitionFilePath : partitionFilePathList)
        {
            totalCountForWord = totalCountForWord + ioUtils.getCountFromFile(partitionFilePath);
        }
        return totalCountForWord;
    }
}
