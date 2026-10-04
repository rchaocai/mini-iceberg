package com.iceberglearn.actions;

import com.iceberglearn.manifests.DeleteManifestEntries;
import com.iceberglearn.manifests.ManifestContent;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.operations.TableOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Implementation of {@link RemoveOrphanFiles}.
 *
 * <p>The algorithm is intentionally simple — four sequential steps with no
 * parallelism, because the interesting part is <em>correctness</em>, not speed:
 * <ol>
 *   <li><b>Collect references.</b> Walk every snapshot in the current metadata
 *       (both retained and current — no snapshot id is filtered out) and note
 *       every file path that is reachable: manifest lists, manifests, data
 *       files, and delete files. These are the files the table knows about.</li>
 *   <li><b>List the directory.</b> Recursively walk the table's on-disk location
 *       and collect the path of every regular file. This is what physically
 *       exists right now.</li>
 *   <li><b>Difference + safety filters.</b> A file on disk is a candidate orphan
 *       when: (a) it is not in the reference set from step 1, (b) its last
 *       modified timestamp is strictly older than the configured
 *       {@link #olderThan(long)} threshold (newer files might belong to a
 *       concurrent writer whose metadata has not been committed yet), and
 *       (c) it is not a {@code *.metadata.json} file — those are versioned
 *       metadata roots and touching them would corrupt the table.</li>
 *   <li><b>Execute.</b> Hand each candidate path to the configured delete
 *       callback. The default callback physically removes the file; a custom
 *       callback can collect the paths for audit or print them for a dry run.</li>
 * </ol>
 *
 * <p>No commit is produced. Orphan files are, by definition, invisible to
 * readers — removing them does not change which rows a scan returns, so there
 * is no metadata change to make atomic.
 */
public class BaseRemoveOrphanFiles implements RemoveOrphanFiles {

    /** Default orphan-age threshold: three days in milliseconds. */
    static final long DEFAULT_OLDER_THAN_MS = 3L * 24L * 60L * 60L * 1000L;

    private final TableOperations operations;

    private long olderThanMillis = System.currentTimeMillis() - DEFAULT_OLDER_THAN_MS;
    private Consumer<String> deleteFunc;

    public BaseRemoveOrphanFiles(TableOperations operations) {
        this.operations = operations;
    }

    @Override
    public RemoveOrphanFiles olderThan(long timestampMillis) {
        this.olderThanMillis = timestampMillis;
        return this;
    }

    @Override
    public RemoveOrphanFiles deleteWith(Consumer<String> func) {
        this.deleteFunc = func;
        return this;
    }

    @Override
    public Result execute() {
        Consumer<String> deleter = deleteFunc != null ? deleteFunc : this::deleteFile;
        TableMetadata base = operations.refresh();
        String location = base.location();

        // Step 1: every file the metadata tree refers to, across every snapshot.
        Set<String> referenced = collectReferencedFiles(base);
        System.out.printf("  [orphan] referenced files: %d%n", referenced.size());

        // Step 2: every regular file currently on disk under the table location.
        List<String> onDisk = listRegularFiles(location);
        System.out.printf("  [orphan] files on disk:  %d%n", onDisk.size());

        // Step 3: apply the three-way safety filter.
        List<String> orphans = new ArrayList<>();
        for (String absolutePath : onDisk) {
            String relative = relativize(location, absolutePath);
            if (referenced.contains(relative) || referenced.contains(absolutePath)) {
                continue;
            }
            Path p = Path.of(absolutePath);
            String name = p.getFileName().toString();
            if (name.endsWith(".metadata.json")) {
                // Never touch metadata roots.
                continue;
            }
            try {
                BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
                if (attrs.lastModifiedTime().toMillis() >= olderThanMillis) {
                    // Too new — could belong to a concurrent write in progress.
                    continue;
                }
            } catch (IOException e) {
                // File vanished between listing and stat — it's already gone.
                continue;
            }
            orphans.add(absolutePath);
        }
        System.out.printf("  [orphan] candidates after filters: %d%n", orphans.size());

        // Step 4: hand each orphan to the delete callback.
        List<String> acted = new ArrayList<>(orphans.size());
        for (String path : orphans) {
            deleter.accept(path);
            acted.add(path);
        }

        return new Result() {
            @Override public Iterable<String> orphanFileLocations() { return acted; }
        };
    }

    // ---------- helpers -----------------------------------------------------

    /**
     * Collect every file path referenced by the metadata. The returned set
     * contains the <em>relative</em> paths used inside metadata JSON (e.g.
     * {@code "metadata/manifest-...json"}) as well as the <em>absolute</em>
     * table-rooted forms — the list-regular-files step returns absolute
     * paths, so we check both.
     */
    private static Set<String> collectReferencedFiles(TableMetadata base) {
        Set<String> referenced = new HashSet<>();
        for (Snapshot snap : base.snapshots()) {
            addReferenced(referenced, base.location(), snap.manifestListLocation());
            ManifestList ml;
            try {
                ml = ManifestFiles.readManifestList(
                        base.location(), snap.manifestListLocation());
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to read manifest list " + snap.manifestListLocation(), e);
            }
            for (ManifestFile mf : ml.manifests()) {
                addReferenced(referenced, base.location(), mf.path());
                if (mf.content() == ManifestContent.DATA) {
                    ManifestEntries entries;
                    try {
                        entries = ManifestFiles.readManifest(base.location(), mf.path());
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to read manifest " + mf.path(), e);
                    }
                    for (ManifestEntry entry : entries.entries()) {
                        DataFile df = entry.dataFile();
                        if (df != null) addReferenced(referenced, base.location(), df.path());
                    }
                } else if (mf.content() == ManifestContent.DELETES) {
                    DeleteManifestEntries entries;
                    try {
                        entries = ManifestFiles.readDeleteManifest(
                                base.location(), mf.path());
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to read delete manifest " + mf.path(), e);
                    }
                    for (var entry : entries.entries()) {
                        DeleteFile df = entry.deleteFile();
                        if (df != null) addReferenced(referenced, base.location(), df.path());
                    }
                }
            }
        }
        return referenced;
    }

    private static void addReferenced(Set<String> out, String tableLocation, String file) {
        out.add(file);
        Path resolved = resolve(tableLocation, file);
        out.add(resolved.toString());
    }

    private static List<String> listRegularFiles(String location) {
        List<String> files = new ArrayList<>();
        Path root = Path.of(location);
        if (!Files.isDirectory(root)) {
            return files;
        }
        try (Stream<Path> stream = Files.walk(root, FileVisitOption.FOLLOW_LINKS)) {
            stream.filter(Files::isRegularFile)
                    .map(Path::toString)
                    .forEach(files::add);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list files under " + root, e);
        }
        return files;
    }

    private static String relativize(String tableLocation, String absolutePath) {
        Path root = Path.of(tableLocation);
        Path p = Path.of(absolutePath);
        try {
            return root.relativize(p).toString();
        } catch (IllegalArgumentException e) {
            return absolutePath;
        }
    }

    private static Path resolve(String tableLocation, String file) {
        Path p = Path.of(file);
        return p.isAbsolute() ? p : Path.of(tableLocation).resolve(p);
    }

    private void deleteFile(String path) {
        try {
            Files.deleteIfExists(Path.of(path));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete " + path, e);
        }
    }
}
