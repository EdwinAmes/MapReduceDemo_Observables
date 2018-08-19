package com.ps.mapreducedemo;

import com.ps.mapreducedemo.util.FileSplitter;
import com.ps.mapreducedemo.map.LineHistogramMaker;
import com.ps.mapreducedemo.reduce.WordCountCalculator;
import com.ps.mapreducedemo.util.IoUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MapReduceDemo {
    static Logger logger = LogManager.getLogger(MapReduceDemo.class);

    public static String INPUT_FOLDER = "input";
    public static String MAP_FOLDER = "mapped";
    public static String REDUCE_FOLDER = "reduced";

    private FileSplitter fileSplitter;
    private LineHistogramMaker lineHistogramMaker;
    private WordCountCalculator wordCountCalculator;
    private IoUtils ioUtils;

    public MapReduceDemo(FileSplitter fileSplitter,
                         LineHistogramMaker lineHistogramMaker,
                         WordCountCalculator wordCountCalculator,
                         IoUtils ioUtils) {
        this.fileSplitter = fileSplitter;
        this.lineHistogramMaker = lineHistogramMaker;
        this.wordCountCalculator = wordCountCalculator;
        this.ioUtils = ioUtils;
    }

    private Path basePath = null;

    private boolean cleanupOldResults() {
        Path mapPath = basePath.resolve(MapReduceDemo.MAP_FOLDER);
        try {
            FileUtils.cleanDirectory(mapPath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Map Folder {}", mapPath);
            return false;
        }

        Path reducePath = basePath.resolve(MapReduceDemo.REDUCE_FOLDER);
        try {
            FileUtils.cleanDirectory(reducePath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Reduce Folder {}", reducePath);
            return false;
        }
        return true;
    }

    /**
     * Entry point.
     * Takes the working filepath and the input file to pull from the INPUT_FOLDER subfolder
     * @param rootPath
     * @param fileToProcess
     */
    public void doMapReduce(String rootPath, String fileToProcess) {
        basePath = ioUtils.loadBasePath(rootPath);
        if (basePath == null) {
            return;
        }

        logger.trace("Cleanup Old Results");
        if (!cleanupOldResults()) return;

        Path inputPath = basePath.resolve(Paths.get(MapReduceDemo.INPUT_FOLDER));
        ioUtils.ensureFolderExists(inputPath);

        Path mapPath = basePath.resolve(Paths.get(MapReduceDemo.MAP_FOLDER));
        ioUtils.ensureFolderExists(mapPath);

        Path reducePath = basePath.resolve(Paths.get(MapReduceDemo.REDUCE_FOLDER));
        ioUtils.ensureFolderExists(reducePath);

        List<String> wordList = fileSplitter.getLinesFromPath(inputPath.resolve(fileToProcess))
                .flatMap(l -> lineHistogramMaker.getHistogramForLine(l))
                .doOnNext(w ->
                {
                    logger.info("line: {} word: {}", w.getLineId(), w.getWord());
                    try {
                        Files.write(
                                mapPath.resolve(w.getWord() + "." + w.getLineId() + ".mp"),
                                Long.toString(w.getCount()).getBytes());
                    } catch (IOException e) {
                        logger.error("Unable to store file for line: {} word: {}", w.getLineId(), w.getWord());
                    }
                })
                .map(w -> w.getWord())
                .distinct()
                .collectList().block();

        // Block to ensure all files written before reduce step

        Flux.fromStream(wordList.stream())
                .doOnNext(s ->
                        {
                            long totalCountForCurrentWord = wordCountCalculator.sumWordCounts(s, mapPath);
                            String fileName = s + "." + totalCountForCurrentWord + ".cnt";
                            try {
                                Files.write(reducePath.resolve(fileName),
                                        Long.toString(totalCountForCurrentWord).getBytes());
                            } catch (IOException e) {
                                logger.error("Error writing file: {}", fileName);
                            }
                        }
                )
                .blockLast();
    }
}
