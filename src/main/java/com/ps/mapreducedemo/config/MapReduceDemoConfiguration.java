package com.ps.mapreducedemo.config;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.util.FileSplitter;
import com.ps.mapreducedemo.map.LineHistogramMaker;
import com.ps.mapreducedemo.reduce.WordCountCalculator;
import com.ps.mapreducedemo.util.IoUtils;
import com.ps.mapreducedemo.util.IoUtilsImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class MapReduceDemoConfiguration {
    private FileSplitter fileSplitter;
    private LineHistogramMaker lineHistogramMaker;
    private WordCountCalculator wordCountCalculator;
    private IoUtils ioUtils;

    public MapReduceDemoConfiguration(@Lazy FileSplitter fileSplitter,
                                      @Lazy LineHistogramMaker lineHistogramMaker,
                                      @Lazy WordCountCalculator wordCountCalculator,
                                      @Lazy IoUtils ioUtils) {
        this.fileSplitter = fileSplitter;
        this.lineHistogramMaker = lineHistogramMaker;
        this.wordCountCalculator = wordCountCalculator;
        this.ioUtils = ioUtils;
    }

    @Bean
    public MapReduceDemo newMapReduceDemo(){
        return new MapReduceDemo(fileSplitter,lineHistogramMaker, wordCountCalculator, ioUtils);
    }

    @Bean
    public FileSplitter fileSplitter(){
        return new FileSplitter();
    }

    @Bean
    public LineHistogramMaker lineHistogramMaker(){
        return new LineHistogramMaker();
    }

    @Bean
    public IoUtils ioUtils(){
        return new IoUtilsImpl();
    }

    @Bean
    public WordCountCalculator wordReducer() {
        return new WordCountCalculator(ioUtils);
    }
}
