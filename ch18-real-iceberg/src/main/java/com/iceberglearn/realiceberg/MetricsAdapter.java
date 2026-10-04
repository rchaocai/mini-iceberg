package com.iceberglearn.realiceberg;

import com.iceberglearn.metadata.DataFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 把 mini-Iceberg 的 {@link DataFile} 里的统计信息，适配成真实 Iceberg {@link Metrics}
 * 需要的形态。两者结构一样，但边界有两处差异：
 * 1. 真实版 bounds 是 {@link ByteBuffer} 序列，mini-Iceberg 保留 Java 原生对象（Long/String/Integer）；
 * 2. 真实版额外有 {@code nanValueCounts}（浮点 NaN 计数），mini-Iceberg 演示用不到。
 */
final class MetricsAdapter {

    /**
     * 本地与真实 Schema 字段顺序一致：
     * 1=id(Long), 2=name(String), 3=age(Integer), 4=city(String, optional)。
     */
    private static final Map<Integer, Type> FIELD_TYPES = Map.of(
            1, Types.LongType.get(),
            2, Types.StringType.get(),
            3, Types.IntegerType.get(),
            4, Types.StringType.get()
    );

    private final DataFile mini;

    MetricsAdapter(DataFile mini) {
        this.mini = mini;
    }

    Metrics toIcebergMetrics() {
        return new Metrics(
                mini.recordCount(),
                mini.columnSizes(),
                mini.valueCounts(),
                mini.nullValueCounts(),
                /* nanValueCounts = */ null,
                encodeBounds(mini.lowerBounds()),
                encodeBounds(mini.upperBounds())
        );
    }

    private Map<Integer, ByteBuffer> encodeBounds(Map<Integer, Object> bounds) {
        if (bounds == null || bounds.isEmpty()) return null;
        Map<Integer, ByteBuffer> out = new HashMap<>();
        bounds.forEach((id, value) -> {
            if (value == null) return;
            Type type = FIELD_TYPES.get(id);
            if (type != null) {
                out.put(id, Conversions.toByteBuffer(type, coerce(type, value)));
            }
        });
        return out;
    }

    /**
     * mini-Iceberg 的 footer 统计读取器把所有数值边界统一归成 Long（省掉谓词比较侧的类型分支），
     * 但真实版 Conversions.toByteBuffer 按 field 的原始类型要求 Integer 字段就是 Integer、
     * Long 字段就是 Long，所以这里按目标 Type 做一次精确转换。
     */
    private static Object coerce(Type type, Object value) {
        if (!(value instanceof Number n)) return value;
        return switch (type.typeId()) {
            case INTEGER, DATE -> n.intValue();
            case LONG, TIME, TIMESTAMP, TIMESTAMP_NANO -> n.longValue();
            case FLOAT -> n.floatValue();
            case DOUBLE -> n.doubleValue();
            default -> value;
        };
    }
}
