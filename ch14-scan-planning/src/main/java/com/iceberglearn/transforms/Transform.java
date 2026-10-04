package com.iceberglearn.transforms;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.iceberglearn.expressions.Expr;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A function from a source column value to a partition value.
 *
 * <p>The set of transforms is intentionally closed (sealed): partition planning dispatches
 * on the concrete type with instanceof, and the compiler guarantees no case is missed.
 */
@JsonSerialize(using = Transform.TransformSerializer.class)
@JsonDeserialize(using = Transform.TransformDeserializer.class)
public sealed interface Transform permits Identity, Bucket, Truncate, Days, Hours {

    /** Apply this transform to a source column value, producing a partition value. */
    Object apply(Object value);

    /**
     * Project one source-column predicate to an inclusive predicate on a partition field.
     *
     * <p>Returning {@code null} means this transform cannot derive a useful safe predicate. The
     * caller must then keep the partition rather than risk skipping matching rows.
     */
    Expr project(int partitionFieldId, Expr sourcePredicate);

    /** True when this transform leaves the value unchanged (used by query planning). */
    boolean isIdentity();

    /**
     * Reconstruct a transform from its string form, as serialized in metadata.json.
     * "identity", "day", "hour", "bucket[4]", "truncate[10]".
     */
    static Transform fromString(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("Transform string must not be blank");
        }
        if ("identity".equals(spec)) {
            return Identity.get();
        }
        if ("day".equals(spec)) {
            return Days.get();
        }
        if ("hour".equals(spec)) {
            return Hours.get();
        }

        Matcher bracketed = Pattern.compile("(\\w+)\\[(\\d+)]").matcher(spec);
        if (bracketed.matches()) {
            String name = bracketed.group(1);
            int width = Integer.parseInt(bracketed.group(2));
            if ("bucket".equals(name)) {
                return Bucket.get(width);
            }
            if ("truncate".equals(name)) {
                return Truncate.get(width);
            }
        }
        throw new IllegalArgumentException("Unknown transform: " + spec);
    }

    // Jackson serializer/deserializer so Transform is written as its string form in metadata.json,
    // following the "transform": "day" / "bucket[4]" convention.
    final class TransformSerializer extends JsonSerializer<Transform> {
        @Override
        public void serialize(Transform value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value == null ? null : value.toString());
        }
    }

    final class TransformDeserializer extends JsonDeserializer<Transform> {
        @Override
        public Transform deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            String text = p.getValueAsString();
            return text == null ? null : Transform.fromString(text);
        }
    }
}
