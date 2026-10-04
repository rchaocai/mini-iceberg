package com.iceberglearn.operations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.iceberglearn.exceptions.AlreadyExistsException;
import com.iceberglearn.exceptions.ValidationException;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metadata.TableMetadataParser;




/** TableOperations implementation for a table stored below one file-system location. */
public class FileSystemTableOperations implements TableOperations {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemTableOperations.class);
    public static final String VERSION_HINT_FILENAME = "version-hint.text";

    private static final String METADATA_DIRECTORY = "metadata";
    private static final Pattern VERSION_PATTERN = Pattern.compile("v(\\d+)\\.metadata\\.json");

    private final Path tableLocation;

    private volatile TableMetadata currentMetadata;
    private volatile Integer version;
    private volatile boolean shouldRefresh = true;

    public FileSystemTableOperations(Path tableLocation) {
        this.tableLocation = tableLocation.toAbsolutePath().normalize();
    }

    @Override
    public TableMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    @Override
    public synchronized TableMetadata refresh() {
        int currentVersion = version != null ? version : findVersion();
        Path metadataFile = getMetadataFile(currentVersion);

        if (version == null && currentVersion == 0 && metadataFile == null) {
            shouldRefresh = false;
            return null;
        }
        if (metadataFile == null) {
            throw new ValidationException(
                    "Metadata file for version " + currentVersion + " is missing under " + metadataRoot());
        }

        Path nextMetadataFile = getMetadataFile(currentVersion + 1);
        while (nextMetadataFile != null) {
            currentVersion += 1;
            metadataFile = nextMetadataFile;
            nextMetadataFile = getMetadataFile(currentVersion + 1);
        }

        try {
            TableMetadata refreshed = TableMetadataParser.read(metadataFile);
            checkTableUuid(refreshed);
            version = currentVersion;
            currentMetadata = refreshed;
            shouldRefresh = false;
            return currentMetadata;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to refresh table at " + tableLocation, e);
        }
    }

    @Override
    public synchronized void commit(TableMetadata base, TableMetadata metadata) {
        if (base == metadata) {
            LOG.info("Nothing to commit for table at {}", tableLocation);
            return;
        }

        int nextVersion = (version != null ? version : findVersion()) + 1;
        Path tempMetadataFile = writeTemporaryMetadata(metadata);
        Path finalMetadataFile = metadataFile(nextVersion);

        System.out.println("  [commit] write temporary metadata: " + tempMetadataFile.getFileName());
        renameToFinal(tempMetadataFile, finalMetadataFile);

        // The version hint is only a shortcut; the versioned metadata file is the commit point.
        writeVersionHint(nextVersion);
        version = nextVersion;
        currentMetadata = metadata;
        shouldRefresh = false;
        System.out.println("  [commit] committed " + finalMetadataFile.getFileName());
    }

    Path writeTemporaryMetadata(TableMetadata metadata) {
        Path tempMetadataFile = metadataRoot().resolve(UUID.randomUUID() + ".metadata.json");
        try {
            Files.createDirectories(metadataRoot());
            TableMetadataParser.write(tempMetadataFile, metadata, StandardOpenOption.CREATE_NEW);
            return tempMetadataFile;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write temporary metadata at " + tempMetadataFile, e);
        }
    }

    protected void renameToFinal(Path source, Path target) {
        try {
            if (Files.exists(target)) {
                throw new ValidationException("Metadata version already exists: " + target);
            }
            System.out.println("  [commit] atomic rename: " + source.getFileName()
                    + " -> " + target.getFileName());
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            tryDelete(source, e);
            throw new UncheckedIOException("Failed to commit metadata at " + target, e);
        } catch (RuntimeException e) {
            tryDelete(source, e);
            throw e;
        }
    }

    public void create(TableMetadata metadata) {
        if (current() != null) {
            throw new AlreadyExistsException("Table already exists at: " + tableLocation);
        }

        Path metadataFile = metadataFile(1);
        try {
            Files.createDirectories(metadataRoot());
            TableMetadataParser.write(metadataFile, metadata, StandardOpenOption.CREATE_NEW);
        } catch (FileAlreadyExistsException e) {
            throw new AlreadyExistsException("Table already exists at: " + tableLocation);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create table at " + tableLocation, e);
        }

        writeVersionHint(1);
        version = 1;
        currentMetadata = metadata;
        shouldRefresh = false;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        return metadataRoot().resolve(fileName).toString();
    }

    @Override
    public String currentMetadataLocation() {
        Integer currentVersion = version;
        return currentVersion == null ? null : metadataFile(currentVersion).toString();
    }

    int findVersion() {
        try {
            return Integer.parseInt(Files.readString(versionHintFile()).trim());
        } catch (Exception ignored) {
            return findLatestVersion();
        }
    }

    Path versionHintFile() {
        return metadataRoot().resolve(VERSION_HINT_FILENAME);
    }

    private int findLatestVersion() {
        if (!Files.isDirectory(metadataRoot())) {
            return 0;
        }

        try (var files = Files.list(metadataRoot())) {
            return files
                    .map(path -> version(path.getFileName().toString()))
                    .filter(candidate -> candidate > 0)
                    .filter(candidate -> getMetadataFile(candidate) != null)
                    .max(Integer::compareTo)
                    .orElse(0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to inspect metadata under " + metadataRoot(), e);
        }
    }

    private Path getMetadataFile(int metadataVersion) {
        if (metadataVersion <= 0) {
            return null;
        }
        Path metadataFile = metadataFile(metadataVersion);
        return Files.isRegularFile(metadataFile) ? metadataFile : null;
    }

    private Path metadataFile(int metadataVersion) {
        return metadataRoot().resolve("v" + metadataVersion + ".metadata.json");
    }

    private Path metadataRoot() {
        return tableLocation.resolve(METADATA_DIRECTORY);
    }

    private int version(String fileName) {
        Matcher matcher = VERSION_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return -1;
        }
        try {
            return Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void writeVersionHint(int versionToWrite) {
        Path tempHint = metadataRoot().resolve(UUID.randomUUID() + "-version-hint.temp");
        try {
            Files.writeString(tempHint, Integer.toString(versionToWrite), StandardOpenOption.CREATE_NEW);
            try {
                Files.move(
                        tempHint,
                        versionHintFile(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempHint, versionHintFile(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ignored) {
            try {
                Files.deleteIfExists(tempHint);
            } catch (IOException cleanupFailure) {
                ignored.addSuppressed(cleanupFailure);
            }
            LOG.warn("Failed to update version hint at {}", versionHintFile(), ignored);
        }
    }

    private void tryDelete(Path path, Exception failure) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void checkTableUuid(TableMetadata refreshed) {
        if (currentMetadata != null && !currentMetadata.tableUuid().equals(refreshed.tableUuid())) {
            throw new ValidationException(
                    "Table UUID does not match: current=" + currentMetadata.tableUuid()
                            + " != refreshed=" + refreshed.tableUuid());
        }
    }
}
