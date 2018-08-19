package com.ps.mapreducedemo.util;

import com.ps.mapreducedemo.domain.Line;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.BaseStream;

/**
 * Pull a text file apart into separate lines. Assumes exclusive file access.
 * Trims leading and trailing spaces from all lines
 * Generates unique id for each #{{@link Line}} object
 */
public class FileSplitter {
    static Logger logger = LogManager.getLogger(FileSplitter.class);

    private AtomicLong nextLineId = new AtomicLong(1);

    private long getNextLineId(){
        return nextLineId.getAndIncrement();
    }

    /**
     * Adapted from https://gist.github.com/simonbasle/0167a1f833a19724646bc7eb27e4346b
     * Returns ordered stream of lines: lines() is ordered, filter and map do not change ordering
     * @param path
     * @return
     */
    public Flux<Line> getLinesFromPath(Path path) {
        return Flux.using(() -> Files.lines(path)
                        .filter(Objects::nonNull)
                        .map(t->t.trim())
                        .map(s->new Line(getNextLineId(),s)),
                Flux::fromStream,
                BaseStream::close
        );
    }
}
