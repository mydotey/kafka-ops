package org.mydotey.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author koqizhao
 *
 * Dec 10, 2018
 */
public interface FileUtil {

    static String readFileContent(Path file) throws IOException {
        return readFileContent(file, StandardCharsets.UTF_8);
    }

    static String readFileContent(Path file, Charset charset) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        Files.readAllLines(file, charset).forEach(l -> stringBuilder.append(l).append(System.lineSeparator()));
        return stringBuilder.toString();
    }

}
