package com.ps.mapreducedemo.reduce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.ps.mapreducedemo.util.MapReduceDemoUtil.getSubPaths;

/**
 * Reduces mapped files: sums the mapped files into final word count files - one per word/partition.
 */
public class WordReducer {
    static Logger logger = LogManager.getLogger(WordReducer.class);

    /**
     * Read all partition file for specified word. Add up the word counts.
     * @param word
     * @param inputPath
     */
    public long sumWordCounts(String word, Path inputPath) {
        long totalCountForWord = 0;
        List<Path> partitionFileList = getSubPaths(inputPath, word + ".*.mp");
        for(Path partitionFile : partitionFileList)
        {
            totalCountForWord = totalCountForWord + getCurrentCountForWord(partitionFile);
        }
        return totalCountForWord;
    }

    private long getCurrentCountForWord(Path wordFilePath) {
        long currentCountForWord = 0;
        File wordFile = wordFilePath.toFile();

        if(wordFile.exists())
        {
            try {
                String currentCountAsString = new String(Files.readAllBytes(wordFilePath));
                currentCountForWord = Long.parseLong(currentCountAsString);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return currentCountForWord;
    }
}
