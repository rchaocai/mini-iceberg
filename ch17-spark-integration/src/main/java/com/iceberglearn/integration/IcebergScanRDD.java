package com.iceberglearn.integration;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.ParquetReader;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.scan.CombinedScanTask;
import com.iceberglearn.scan.FileScanTask;
import com.iceberglearn.schema.Schema;

import com.sparklearn.core.Dependency;
import com.sparklearn.core.Partition;
import com.sparklearn.core.SparkContext;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.And;
import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.expressions.EqualTo;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.GreaterThan;
import com.sparklearn.sql.catalyst.expressions.GreaterThanOrEqual;
import com.sparklearn.sql.catalyst.expressions.LessThan;
import com.sparklearn.sql.catalyst.expressions.LessThanOrEqual;
import com.sparklearn.sql.catalyst.expressions.Literal;
import com.sparklearn.sql.catalyst.expressions.NotEqualTo;
import com.sparklearn.sql.catalyst.expressions.Or;
import com.sparklearn.sql.sources.SupportsPushdownFilters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The single bridge between mini-Iceberg and mini-Spark. Extends mini-Spark's
 * {@link RDD} and implements {@code getPartitionsInternal} (ScanTask → Partition)
 * and {@code compute} (read Parquet → Row).
 *
 * <p>Implements {@link SupportsPushdownFilters} so that {@link
 * com.sparklearn.sql.execution.ScanExec} can hand over the predicates that the
 * optimizer pushed into the Scan node. The RDD converts each Spark
 * {@link Expression} into an Iceberg {@link Expr} and passes it to
 * {@link IcebergScanSource#scan} for file-level pruning (metrics-based file
 * skipping). Row-level residual filtering is still done by ScanExec.
 */
public class IcebergScanRDD extends RDD<Row> implements SupportsPushdownFilters {

    private final IcebergScanSource source;
    private final List<String> fieldNames;
    private final List<Integer> fieldIds;
    private Expr pushedFilter = null;
    private transient List<CombinedScanTask> plannedTasks;

    public IcebergScanRDD(SparkContext sparkContext, IcebergScanSource source) {
        super(sparkContext);
        this.source = source;
        this.fieldNames = new ArrayList<>();
        this.fieldIds = new ArrayList<>();
        for (Schema.NestedField nf : source.icebergSchema().fields()) {
            fieldNames.add(nf.name());
            fieldIds.add(nf.id());
        }
    }

    @Override
    public void pushFilters(List<Expression> filters) {
        Expr combined = null;
        for (Expression expr : filters) {
            Expr converted = convertToIcebergExpr(expr);
            if (converted != null) {
                combined = (combined == null) ? converted : Exprs.and(combined, converted);
            }
        }
        this.pushedFilter = combined;
    }

    /**
     * Convert a Spark {@link Expression} tree into an Iceberg {@link Expr}.
     * Returns {@code null} for unsupported expression types — the filter will
     * simply not be pushed down, and row-level evaluation in ScanExec still
     * guarantees correct results.
     */
    private Expr convertToIcebergExpr(Expression expr) {
        if (expr instanceof GreaterThan gt) {
            Integer fieldId = resolveFieldId(gt.left());
            Object value = resolveLiteral(gt.right());
            if (fieldId != null && value != null) return Exprs.greaterThan(fieldId, value);
        }
        if (expr instanceof GreaterThanOrEqual gte) {
            Integer fieldId = resolveFieldId(gte.left());
            Object value = resolveLiteral(gte.right());
            if (fieldId != null && value != null) return Exprs.greaterThanOrEqual(fieldId, value);
        }
        if (expr instanceof LessThan lt) {
            Integer fieldId = resolveFieldId(lt.left());
            Object value = resolveLiteral(lt.right());
            if (fieldId != null && value != null) return Exprs.lessThan(fieldId, value);
        }
        if (expr instanceof LessThanOrEqual lte) {
            Integer fieldId = resolveFieldId(lte.left());
            Object value = resolveLiteral(lte.right());
            if (fieldId != null && value != null) return Exprs.lessThanOrEqual(fieldId, value);
        }
        if (expr instanceof EqualTo eq) {
            Integer fieldId = resolveFieldId(eq.left());
            Object value = resolveLiteral(eq.right());
            if (fieldId != null && value != null) return Exprs.equal(fieldId, value);
        }
        if (expr instanceof NotEqualTo neq) {
            Integer fieldId = resolveFieldId(neq.left());
            Object value = resolveLiteral(neq.right());
            if (fieldId != null && value != null) return Exprs.notEqual(fieldId, value);
        }
        if (expr instanceof And and) {
            Expr left = convertToIcebergExpr(and.left());
            Expr right = convertToIcebergExpr(and.right());
            if (left != null && right != null) return Exprs.and(left, right);
        }
        if (expr instanceof Or or) {
            Expr left = convertToIcebergExpr(or.left());
            Expr right = convertToIcebergExpr(or.right());
            if (left != null && right != null) return Exprs.or(left, right);
        }
        return null; // unsupported — skip pushdown, rely on row-level filter
    }

    private Integer resolveFieldId(Expression expr) {
        if (expr instanceof Attribute attr) {
            int idx = fieldNames.indexOf(attr.name());
            if (idx >= 0) return fieldIds.get(idx);
        }
        return null;
    }

    private Object resolveLiteral(Expression expr) {
        if (expr instanceof Literal lit) {
            return lit.value();
        }
        return null;
    }

    @Override
    protected List<Partition> getPartitionsInternal() {
        if (plannedTasks == null) {
            plannedTasks = source.scan(pushedFilter);
        }
        List<Partition> partitions = new ArrayList<>(plannedTasks.size());
        for (int i = 0; i < plannedTasks.size(); i++) {
            partitions.add(new Partition(i));
        }
        return partitions;
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return List.of();
    }

    @Override
    public Iterator<Row> compute(Partition partition) {
        CombinedScanTask task = plannedTasks.get(partition.index());
        List<Row> rows = new ArrayList<>();
        for (FileScanTask fileTask : task.files()) {
            DataFile df = fileTask.file();
            List<Map<Integer, Object>> records = ParquetReader.read(
                    source.icebergSchema(), Path.of(source.location(), df.path()));
            for (Map<Integer, Object> record : records) {
                Object[] values = new Object[fieldIds.size()];
                for (int i = 0; i < fieldIds.size(); i++) {
                    values[i] = record.get(fieldIds.get(i));
                }
                Row row = Row.apply(values);
                row = row.withSchema(source.sparkSchema());
                rows.add(row);
            }
        }
        return rows.iterator();
    }
}
