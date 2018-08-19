package com.ps.mapreducedemo.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;

public class IoUtilsImpl implements IoUtils {
    static Logger logger = LogManager.getLogger(IoUtilsImpl.class);

    // Reading short files with standard numbers
    private static Charset charSet = StandardCharsets.US_ASCII;

    @Override
    public long getCountFromFile(Path wordFilePath) {
        long currentCountForWord = 0;
        File wordFile = wordFilePath.toFile();

        if(wordFile.exists())
        {
            try {
                String currentCountAsString = readFileContents(wordFilePath);
                currentCountForWord = Long.parseLong(currentCountAsString);
            } catch (IOException e) {
                logger.error("Unable to read file: {}", wordFilePath);
            }
        }
        return currentCountForWord;
    }

    @Override
    public void ensureFolderExists(Path folderPath) {
        File outputFolder = folderPath.toFile();
        if(!outputFolder.exists())
            outputFolder.mkdir();
    }

    @Override
    public boolean overwriteFile(Path filePath, String content) throws IOException {
        try (OutputStream out = new BufferedOutputStream(
                Files.newOutputStream(filePath, WRITE, TRUNCATE_EXISTING, CREATE))) {
            byte[] contentData = content.getBytes(charSet);
            out.write(contentData, 0, contentData.length);
        }
        return true;
    }

    @Override
    public String readFileContents(Path filePath)  throws IOException {
        return new String(Files.readAllBytes(filePath), charSet);
    }

    @Override
    public void cleanDirectory(Path pathToClean) throws IOException{
        org.apache.commons.io.FileUtils.cleanDirectory(pathToClean.toFile());
    }

    // Path related functionality

    @Override
    public Path loadBasePath(String rootPath){
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
    @Override
    public List<Path> getSubPaths(Path path, String filter) {
        List<Path> subPathList = new ArrayList<Path>();
        if(path==null)
            return subPathList;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, filter)) {
            for (Path file : stream) {
                subPathList.add(file);
            }
        } catch (IOException e) {
            logger.error("Error Loading Files in Path {}", path);
        }
        return subPathList;
    }

    @Override
    public Path resolvePath(Path basePath, String subPath){
        return basePath.resolve(Paths.get(subPath));
    }
}
