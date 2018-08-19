package com.ps.mapreducedemo.map;

import static org.assertj.core.api.Assertions.*;

import com.ps.mapreducedemo.domain.Line;
import com.ps.mapreducedemo.domain.WordFrequency;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;

import java.util.ArrayList;

/**
 * Created by Edwin on 4/20/2016.
 */
public class LineHistogramTest {

    @Before
    public void setup()
    {
        sut = new LineHistogramMaker();
    }

    private LineHistogramMaker sut;

    @Test
    public void when_given_empty_string_returns_nothing()
    {
        StepVerifier.create(sut.getHistogramForLine(new Line(0, "")))
                .verifyComplete();
    }

    @Test
    public void when_given_blank_string_returns_nothing()
    {
        StepVerifier.create(sut.getHistogramForLine(new Line(0, "                         ")))
                .verifyComplete();
    }

    @Test
    public void when_given_string_with_one_word_returns_frequency(){
        StepVerifier.create(sut.getHistogramForLine(new Line(0, "     abc          ")))
                .expectNext(new WordFrequency(0,"abc",1))
                .verifyComplete();
    }

    @Test
    public void when_given_string_with_three_words_returns_frequencies(){
        StepVerifier.create(sut.getHistogramForLine(new Line(0, "     abc   1243    doe    ")))
                .recordWith(ArrayList::new)
                .expectNextCount(3)
                .consumeRecordedWith(results -> {
                    assertThat(results)
                            .contains(
                                    new WordFrequency(0,"abc",1),
                                    new WordFrequency(0,"1243",1),
                                    new WordFrequency(0,"doe",1)
                            );

                })
                .verifyComplete();
    }

    @Test
    public void when_given_string_with_two_words_and_dupe_returns_correct_frequencies(){
        StepVerifier.create(sut.getHistogramForLine(new Line(0, "     abc   1243    abc    ")))
                .recordWith(ArrayList::new)
                .expectNextCount(2)
                .consumeRecordedWith(results -> {
                    assertThat(results)
                            .contains(
                                    new WordFrequency(0,"abc",2),
                                    new WordFrequency(0,"1243",1)
                            );

                })
                .verifyComplete();
    }
}
