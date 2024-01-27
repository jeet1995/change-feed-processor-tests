package com.cfp.runners;

import com.cfp.runners.entity.RequestResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.List;

public class FileUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String basePathAsString = loadBasePathAsString();

    public static void writeRequestResponseEntitiesToFile(
            List<RequestResponseEntity> requestResponseEntities,
            String fileName) {
        try {
            OBJECT_MAPPER.writeValue(new File(Paths.get(basePathAsString, fileName).toString()), requestResponseEntities);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String loadBasePathAsString() {
        return FileSystems.getDefault().getPath("").toAbsolutePath().toString();
    }

}
