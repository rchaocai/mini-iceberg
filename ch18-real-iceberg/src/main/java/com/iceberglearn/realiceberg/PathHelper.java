package com.iceberglearn.realiceberg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 文件辅助：找最新 metadata 版本、清空表目录、为 manifest 路径打印等提供稳定的工具方法。
 */
final class PathHelper {

    private static final Pattern VERSION_FILE = Pattern.compile("v(\\d+)\\.metadata\\.json");
    private static final Pattern SNAP_AVRO = Pattern.compile("snap-\\d+-\\d+-[0-9a-f\\-]+\\.avro");
    private static final Pattern UUID_PARQUET = Pattern.compile("orders-.*-[0-9a-f\\-]+\\.parquet");

    private PathHelper() {
    }

    /**
     * 从 table 目录下的 metadata/ 子目录里挑出版本号最大的 vN.metadata.json，
     * 命名约定与 Iceberg spec 一致，和 mini-Iceberg 第 3 章逻辑相同。
     */
    static Path findLatestMetadata(Path tableRoot) throws IOException {
        Path metadataDir = tableRoot.resolve("metadata");
        if (!Files.isDirectory(metadataDir)) {
            throw new IOException("metadata directory not found: " + metadataDir);
        }
        try (Stream<Path> stream = Files.list(metadataDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(p -> {
                        Matcher m = VERSION_FILE.matcher(p.getFileName().toString());
                        return m.matches() ? new Object[]{p, Integer.parseInt(m.group(1))} : null;
                    })
                    .filter(arr -> arr != null)
                    .max(Comparator.comparingInt(a -> (int) a[1]))
                    .map(arr -> (Path) arr[0])
                    .orElseThrow(() -> new IOException("no vN.metadata.json found in " + metadataDir));
        }
    }

    /** 递归删除整个表目录，保证每次运行 Step 0 都是从空表开始。 */
    static void cleanDir(Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new IllegalStateException("delete failed: " + p, e);
                        }
                    });
        }
    }

    /** 给数据文件名加一个秒级时间戳标签，避免多次运行相互覆盖。 */
    static String timestampTag() {
        return Long.toString(System.currentTimeMillis() / 1000);
    }

    /** manifest-list / 数据文件路径在输出时做适度缩短，长 uuid 保留关键段。 */
    static String abbrev(Path p) {
        String s = p.toString();
        s = UUID_PARQUET.matcher(s).replaceAll("orders-...-[tag].parquet");
        s = SNAP_AVRO.matcher(s).replaceAll("snap-<id>-1-<uuid>.avro");
        return s;
    }
}
