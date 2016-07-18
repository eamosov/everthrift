package io.smartcat.migration;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.smartcat.migration.exceptions.MigrationException;

public class CqlMigration extends Migration {
    
    private static final Pattern filenamePattern = Pattern.compile("^/(.+/)*(.+\\.(.+))$");

    final String resourceName;
    
    public CqlMigration(MigrationType type, String resourceName) {
        super(type);
        this.resourceName = resourceName;
    }
    
    @Override
    public int getVersion(){
        return extractVersion(resourceName);
    }

    public String loadResource(InputStream input) throws IOException {

        String result = null;
        int available = input.available();
        if (available > 0) {
            // Read max 64k. This is a damn lazy implementation...
            final int MAX_BYTES = 65535;

            // Read all available bytes in one chunk
            byte[] buffer = new byte[Math.min(available, MAX_BYTES)];
            int numRead = input.read(buffer);

            result = new String(buffer, 0, numRead, "UTF-8");
        }

        return result;
    }

    private String loadResource(String path) throws IOException {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = getClass().getClassLoader();
        }

        InputStream input = loader.getResourceAsStream(path);
        if (input == null) {
            final File file = new File(path);
            if (file.exists()) {
                input = new FileInputStream(file);
            }
        }

        if (input == null) {
            throw new IllegalArgumentException("Resource \"" + path + "\" not found");
        }

        try {
            input = new BufferedInputStream(input);
            return loadResource(input);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private List<String> loadCQLStatements(String resourceName) throws MigrationException {

        final List<String> statements = new ArrayList<>();

        final String source;
        try {
            source = loadResource(resourceName);
        } catch (IOException e) {
            throw new MigrationException("Could not load resource \"" + resourceName + "\"", e);
        }

        if (source == null) {
            return statements;
        }

        final String[] lines = source.split("\n");

        StringBuilder statement = new StringBuilder();

        for (int i = 0; i < lines.length; i++) {
            int index;
            String line = lines[i];
            String trimmedLine = line.trim();

            if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
                // Skip
            } else if ((index = line.indexOf(";")) != -1) {
                // Split the line at the semicolon
                statement.append("\n").append(line.substring(0, index + 1));
                statements.add(statement.toString());

                if (line.length() > index + 1) {
                    statement = new StringBuilder(line.substring(index + 1));
                } else {
                    statement = new StringBuilder();
                }
            } else {
                statement.append("\n").append(line);
            }
        }

        return statements;
    }

    @Override
    public String getDescription() {        
        final Matcher m = filenamePattern.matcher(resourceName);
        return "CQL:" + (m.matches() ? m.group(2) : resourceName);
    }

    @Override
    public void execute() throws MigrationException {
        for (String s: loadCQLStatements(resourceName)){
            session.execute(s);
        }        
    }
}
