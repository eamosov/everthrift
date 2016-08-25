package org.everthrift.maven;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Goal to generate Migration source file.
 */
@Mojo(name = "generate")
public class GenerateMigrationMojo extends AbstractMojo {

    private static enum PLACEHOLDERS {
        PACKAGE("<package>"),
        VERSION("<version>"),
        MIGRATION_NAME("<migrationName>");

        private String placeholder;

        PLACEHOLDERS(String placeholder) {
            this.placeholder = placeholder;
        }
    }

    @Parameter(defaultValue = "${project.build.sourceDirectory}", required = true)
    private File outputDirectory;

    @Parameter(property = "migration.name", alias = "migration.name", defaultValue = "EnterMigrationNameHere")
    private String migrationName;

    @Parameter(property = "target.package", alias = "target.package", defaultValue = "${migration.target.package}", required = true)
    private String packageName;

    @Parameter(property = "template.file", alias = "template.file", required = true)
    private File templateFile;

    private String packageDir;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        init();
        getLog().info("Start Migration generation");
        getLog().info(String.format("Migration name: %s", getFullMigrationName()));
        getLog().info(String.format("Migration package: %s", packageName));
        getLog().info(String.format("Migration class will be writen to: %s", outputDirectory.toPath().resolve(packageDir)));
        try {
            createMigration();
        }
        catch (IOException e) {
            getLog().error("Error while execute Migration:" + e.getMessage());
            new MojoExecutionException("Error while execute Migration: ", e);
        }
    }

    private void init() {
        packageDir = packageName.replaceAll("\\.", "/");
    }

    private String getFullMigrationName() {
        SimpleDateFormat sdf = new SimpleDateFormat("'m'YYMMdd_HHmmss_");
        return sdf.format(Calendar.getInstance().getTime()) + migrationName;
    }

    private void createMigration() throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(templateFile.getAbsolutePath())));
        content = replacePlaceholders(content);
        outputDirectory = outputDirectory.toPath().resolve(packageDir).toFile();
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }
        File migration = new File(outputDirectory, getFullMigrationName().concat(".java"));
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(migration);
            fileWriter.write(content);
        }
        finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    private String replacePlaceholders(String in) {
        in = in.replaceAll(PLACEHOLDERS.VERSION.placeholder, getFullMigrationName());
        in = in.replaceAll(PLACEHOLDERS.MIGRATION_NAME.placeholder, getFullMigrationName());
        if (packageName != null && packageName.trim().length() > 0) {
            in = in.replaceAll(PLACEHOLDERS.PACKAGE.placeholder, "package ".concat(packageName.concat(";")));
        } else {
            in = in.replaceAll(PLACEHOLDERS.PACKAGE.placeholder, "");
        }
        return in;
    }

}
