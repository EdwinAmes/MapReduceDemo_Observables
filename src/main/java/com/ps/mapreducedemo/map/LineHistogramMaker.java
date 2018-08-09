package com.ps.mapreducedemo.map;

import com.ps.mapreducedemo.domain.Line;
import com.ps.mapreducedemo.domain.WordFrequency;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Counts the words per line
 */
public class LineHistogramMaker {
    Pattern matchNonLettersPattern = Pattern.compile("[^a-zA-Z ]");

    public Flux<WordFrequency> getHistogramForLine(Line line)
    {
        return Flux.just(line.getText())
                .map(t->matchNonLettersPattern.matcher(t).replaceAll(""))
                .map(String::toLowerCase)
                .flatMap(t->Flux.fromArray(t.split(" "))
                        .filter(Objects::nonNull)
                        .filter(word -> {return !word.equals("");})
                        .groupBy(String::toString)
                        .flatMap(
                                group -> Mono.zip(Mono.just(group.key()), group.count())
                        )
                        .map(
                                tuple->new WordFrequency(line.getId(),tuple.getT1(),tuple.getT2())
                        )
                );
    }
}
