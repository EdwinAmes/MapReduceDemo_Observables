package com.ps.mapreducedemo.util;

import com.ps.mapreducedemo.domain.Line;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.test.StepVerifier;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

/**
 * Created by Edwin on 4/20/2016.
 */
public class FileSplitterTest {
    @BeforeClass
    public static void setupEnv()
    {
        ClassLoader classLoader = FileSplitterTest.class.getClassLoader();
        File file = new File(classLoader.getResource("inputFiles/OneLine.txt").getFile());
        basePath = file.getParentFile().toPath();
    }

    private static Path basePath=null;

    private FileSplitter sut = null;

    @Before
    public void setup()
    {
        sut = new FileSplitter();
    }

    @Test
    public void when_file_does_not_exist_returns_empty_optional()
    {
        StepVerifier.create(sut.getLinesFromPath(Paths.get("Not Exist")))
                .expectError(NoSuchFileException.class);
    }

    @Test
    public void when_file_has_no_lines_returns_no_data()
    {
        Path emptyFilePath = basePath.resolve(Paths.get("Empty_NoLines.txt"));
        StepVerifier.create(sut.getLinesFromPath(emptyFilePath))
                .verifyComplete();
    }

    @Test
    public void when_file_has_one_line_returns_one_with_line()
    {
        Path oneLineFile = basePath.resolve(Paths.get("OneLine.txt"));
        StepVerifier.create(sut.getLinesFromPath(oneLineFile))
                .expectNext(new Line(1,"This file has one line"))
                .verifyComplete();
    }

    @Test
    public void when_file_has_three_lines_returns_list_all_lines()
    {
        Path threeLineFile = basePath.resolve(Paths.get("ThreeLine.txt"));
        // Source Line Stream is ordered. Flux maintains ordering.
        StepVerifier.create(sut.getLinesFromPath(threeLineFile))
                .expectNext(
                             new Line(1,"This file"),
                             new Line(2,"has"),
                             new Line(3, "3 lines")
                            )
                .verifyComplete();
    }
}
