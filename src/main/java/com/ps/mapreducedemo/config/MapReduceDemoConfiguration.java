package com.ps.mapreducedemo.config;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.map.FileSplitter;
import com.ps.mapreducedemo.map.LineHistogramMaker;
import com.ps.mapreducedemo.reduce.WordReducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class MapReduceDemoConfiguration {
    private FileSplitter fileSplitter;
    private LineHistogramMaker lineHistogramMaker;
    private WordReducer wordReducer;

    public MapReduceDemoConfiguration(@Lazy FileSplitter fileSplitter,
                                      @Lazy LineHistogramMaker lineHistogramMaker,
                                      @Lazy WordReducer wordReducer) {
        this.fileSplitter = fileSplitter;
        this.lineHistogramMaker = lineHistogramMaker;
        this.wordReducer = wordReducer;
    }

    @Bean
    public MapReduceDemo newMapReduceDemo(){
        return new MapReduceDemo(fileSplitter,lineHistogramMaker, wordReducer);
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
    public WordReducer wordReducer() {
        return new WordReducer();
    }
}
