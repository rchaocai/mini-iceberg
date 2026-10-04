package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Main {

    private static final Path DATA_DIR = Paths.get("./data/chB-columnar-formats");
    private static final Path CSV_FILE = DATA_DIR.resolve("sales.csv");
    private static final Path PARQUET_FILE = DATA_DIR.resolve("sales.parquet");
    private static final long AMOUNT_THRESHOLD = 75_000;

    public static void main(String[] args) throws Exception {
        resetDataDirectory();

        List<Sale> sales = sampleSales(20_000);

        System.out.println("=== 附录 B：从行到列 ===\n");
        printLogicalLayout(sales.subList(0, 4));

        ParquetDataWriter.writeCsv(sales, CSV_FILE);
        ParquetDataWriter.writeParquet(sales, PARQUET_FILE);

        System.out.println("\n=== 同一批数据的文件大小 ===");
        System.out.println("CSV:     " + Files.size(CSV_FILE) + " bytes");
        System.out.println("Parquet: " + Files.size(PARQUET_FILE) + " bytes");
        System.out.println("这里只观察这一批数据，不把文件大小当成所有工作负载的通用排名。\n");

        ParquetStructureInspector.Inspection result =
                ParquetStructureInspector.inspect(PARQUET_FILE, AMOUNT_THRESHOLD);

        System.out.println("\n=== 4. 从结构推导查询行为 ===");
        System.out.println("读取全部 6 列涉及的 Column Chunk 压缩字节数: " + result.allColumnsBytes());
        System.out.println("只读 status + amount_cents 涉及的压缩字节数: " + result.projectedBytes());
        System.out.printf("列投影保留约 %.1f%% 的 Column Chunk 字节%n",
                percentage(result.projectedBytes(), result.allColumnsBytes()));
        System.out.println("status（低基数）压缩字节数: " + result.statusBytes());
        System.out.println("customer_email（高基数）压缩字节数: " + result.emailBytes());
        System.out.println("谓词 amount_cents >= " + AMOUNT_THRESHOLD
                + " 需要保留 " + result.matchingRowGroupCount()
                + "/" + result.rowGroupCount() + " 个 Row Group");

        System.out.println("\n=== 5. 结论 ===");
        System.out.println("1. Footer 让读取器先知道文件中有哪些 Row Group 和列统计。");
        System.out.println("2. Column Chunk 让查询只读取需要的列。");
        System.out.println("3. min/max 让查询在解码数据前跳过不可能命中的 Row Group。");
        System.out.println("4. 编码与压缩效果取决于数据分布，低基数列通常更容易压缩。");
        System.out.println("5. Parquet 管的是单个文件；Iceberg 还要把这些统计提升到整张表的元数据层。");
    }

    private static List<Sale> sampleSales(int count) {
        String[] regions = {"east", "north", "south", "west"};
        String[] statuses = {"PAID", "PAID", "PAID", "REFUNDED"};
        List<Sale> sales = new ArrayList<>(count);

        for (int index = 0; index < count; index++) {
            sales.add(new Sale(
                    index + 1L,
                    regions[(index / 500) % regions.length],
                    statuses[index % statuses.length],
                    1 + index % 5,
                    1_000L + index * 5L,
                    "customer-" + index + "@example.com"));
        }
        return sales;
    }

    private static void printLogicalLayout(List<Sale> sales) {
        System.out.println("=== 同一批记录，两种摆法 ===");
        System.out.println("行式：");
        for (Sale sale : sales) {
            System.out.println("  " + sale.orderId() + " | " + sale.region() + " | "
                    + sale.status() + " | " + sale.quantity() + " | "
                    + sale.amountCents() + " | " + sale.customerEmail());
        }

        System.out.println("\n列式（逻辑示意）：");
        System.out.println("  order_id:       " + sales.stream().map(Sale::orderId).toList());
        System.out.println("  region:         " + sales.stream().map(Sale::region).toList());
        System.out.println("  status:         " + sales.stream().map(Sale::status).toList());
        System.out.println("  amount_cents:   " + sales.stream().map(Sale::amountCents).toList());
        System.out.println("  customer_email: " + sales.stream().map(Sale::customerEmail).toList());
    }

    private static double percentage(long part, long total) {
        return total == 0 ? 0 : part * 100.0 / total;
    }

    private static void resetDataDirectory() throws IOException {
        if (Files.exists(DATA_DIR)) {
            try (var paths = Files.walk(DATA_DIR)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
        Files.createDirectories(DATA_DIR);
    }
}
