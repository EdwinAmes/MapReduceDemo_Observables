package com.ps.mapreducedemo;

import com.ps.mapreducedemo.map.FileSplitter;
import com.ps.mapreducedemo.map.LineHistogramMaker;
import com.ps.mapreducedemo.reduce.WordReducer;
import com.ps.mapreducedemo.util.MapReduceDemoUtil;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.ps.mapreducedemo.util.MapReduceDemoUtil.ensureFolderExists;

public class MapReduceDemo {
    public static String INPUT_FOLDER = "input";
    public static String MAP_FOLDER = "mapped";
    public static String REDUCE_FOLDER = "reduced";

    static Logger logger = LogManager.getLogger(MapReduceDemo.class);

    private FileSplitter fileSplitter;
    private LineHistogramMaker lineHistogramMaker;
    private WordReducer wordReducer;

    public MapReduceDemo(FileSplitter fileSplitter,
                         LineHistogramMaker lineHistogramMaker, WordReducer wordReducer) {
        this.fileSplitter = fileSplitter;
        this.lineHistogramMaker = lineHistogramMaker;
        this.wordReducer = wordReducer;
    }

    private Path basePath = null;

    private boolean cleanupOldResults() {
        Path mapPath = basePath.resolve(MapReduceDemo.MAP_FOLDER);
        try {
            FileUtils.cleanDirectory(mapPath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Map Folder {}",mapPath);
            return false;
        }

        Path reducePath = basePath.resolve(MapReduceDemo.REDUCE_FOLDER);
        try {
            FileUtils.cleanDirectory(reducePath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Reduce Folder {}",reducePath);
            return false;
        }
        return true;
    }

    public void doMapReduce(String rootPath) {
        basePath = MapReduceDemoUtil.getBasePath(rootPath);
        if (basePath == null) {
            return;
        }

        logger.trace("Cleanup Old Results");
        if (!cleanupOldResults()) return;

        Path inputPath = basePath.resolve(Paths.get(MapReduceDemo.INPUT_FOLDER));
        ensureFolderExists(inputPath);

        Path mapPath = basePath.resolve(Paths.get(MapReduceDemo.MAP_FOLDER));
        ensureFolderExists(mapPath);

        Path reducePath = basePath.resolve(Paths.get(MapReduceDemo.REDUCE_FOLDER));
        ensureFolderExists(reducePath);

        List<String> wordList = fileSplitter.getLinesFromPath(inputPath.resolve("SampleText.txt"))
                .flatMap(l->lineHistogramMaker.getHistogramForLine(l))
                .doOnNext(w->
                {
                    logger.info("line: {} word: {}", w.getLineId(),w.getWord());
                    try {
                        Files.write(
                                mapPath.resolve(w.getWord()+"."+w.getLineId()+".mp"),
                                Long.toString(w.getCount()).getBytes());
                    } catch (IOException e) {
                        logger.error("Unable to store file for line: {} word: {}", w.getLineId(),w.getWord());
                    }
                })
                .map(w->w.getWord())
                .distinct()
                .collectList().block();

        // Block to ensure all files written before reduce step

        Flux.fromStream(wordList.stream())
                .doOnNext(s->
                        {
                            long totalCountForCurrentWord = wordReducer.sumWordCounts(s, mapPath);
                            String fileName = s+"."+totalCountForCurrentWord+".cnt";
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
