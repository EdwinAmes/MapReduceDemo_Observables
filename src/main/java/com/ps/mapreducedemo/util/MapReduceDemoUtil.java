package com.ps.mapreducedemo.util;

import com.ps.mapreducedemo.MapReduceDemo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class MapReduceDemoUtil {
    static Logger logger = LogManager.getLogger(MapReduceDemo.class);

    public static void ensureFolderExists(Path folderPath) {
        File outputFolder = folderPath.toFile();
        if(!outputFolder.exists())
            outputFolder.mkdir();
    }

    public static Path getBasePath(String rootPath){
        Path basePath = Paths.get(rootPath);
        try {
            basePath = basePath.toRealPath(LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            logger.error("Cannot Find Root Folder {}",rootPath);
            return null;
        }
        return basePath;
    }

    /**
     * Loads first level of child paths. Initially used to load a path for all files in a folder.
     * @param path
     */
    public static List<Path> getSubPaths(Path path, String filter) {
        List<Path> subPathList = new ArrayList<Path>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, filter)) {
            for (Path file : stream) {
                subPathList.add(file);
            }
        } catch (IOException e) {
            logger.error("Error Loading Files in Path {}", path);
        }
        return subPathList;
    }
}
