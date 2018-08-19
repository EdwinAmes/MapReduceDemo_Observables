package com.ps.mapreducedemo.reduce;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.PathMatcher;
import com.ps.mapreducedemo.util.IoUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class WordCountCalculatorTest {
    @Mock
    private IoUtils ioUtils;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        sut = new WordCountCalculator(ioUtils);
    }

    private WordCountCalculator sut;
    /**
     * Mockup files in the mapped folder
     * @param word
     * @param fileCount
     * @return
     */
    private List<Path> mockFilesForWord(String word, int fileCount){
        List<Path> filePaths = new ArrayList<>();
        for(int fileIndex=1;fileIndex<=fileCount;fileIndex++){
            filePaths.add(Paths.get(word+"."+fileIndex+".mp"));
        }
        when(ioUtils.getSubPaths(any(), startsWith(word+"."))).thenReturn(filePaths);
        return filePaths;
    }

    @Test
    public void when_no_files_returns_0(){
        long result = sut.sumWordCounts("not there", Paths.get("Not there"));
        assertThat(result, equalTo(0L));

    }

    @Test
    public void when_3_files_gets_correct_count() {
        // Mockup the mapped folder
        Path mapPath = Paths.get("MapPath");
        when(ioUtils
                .resolvePath(any(), matches(MapReduceDemo.MAP_FOLDER))).thenReturn(mapPath);

        List<Path> pathsOne = mockFilesForWord("One", 3);

        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("One.1"))))
                .thenReturn(2L);
        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("One.2"))))
                .thenReturn(3L);
        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("One.3"))))
                .thenReturn(10L);

        when(ioUtils
                .getSubPaths(any(), matches("One"))).thenReturn(pathsOne);

        assertThat(sut.sumWordCounts("One", mapPath), equalTo(15L));
    }

}
