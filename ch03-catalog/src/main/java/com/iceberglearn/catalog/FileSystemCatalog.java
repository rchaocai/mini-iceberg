package com.iceberglearn.catalog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import com.iceberglearn.exceptions.AlreadyExistsException;
import com.iceberglearn.exceptions.NoSuchTableException;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.operations.FileSystemTableOperations;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/**
 * A file-system catalog that maps table identifiers to locations below one warehouse.
 *
 * <p>The directory layout is warehouse/namespace/table/metadata.
 */
public class FileSystemCatalog implements Catalog {
    private final Path warehouse;

    public FileSystemCatalog(String warehouseLocation) {
        this.warehouse = Paths.get(warehouseLocation).toAbsolutePath().normalize();
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema) {
        FileSystemTableOperations operations = newTableOperations(identifier);
        if (operations.current() != null) {
            throw new AlreadyExistsException("Table already exists: " + identifier);
        }

        String tableLocation = defaultWarehouseLocation(identifier).toString();
        TableMetadata metadata = new TableMetadata(
                2,
                UUID.randomUUID().toString(),
                tableLocation,
                0,
                schema.schemaId(),
                List.of(schema),
                null,
                List.of());

        try {
            operations.create(metadata);
        } catch (AlreadyExistsException e) {
            throw new AlreadyExistsException("Table was created concurrently: " + identifier);
        }
        return new Table(identifier, operations);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        FileSystemTableOperations operations = newTableOperations(identifier);
        if (operations.current() == null) {
            throw new NoSuchTableException("Table does not exist: " + identifier);
        }
        return new Table(identifier, operations);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return newTableOperations(identifier).current() != null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
        FileSystemTableOperations operations = newTableOperations(identifier);
        if (operations.current() == null) {
            return false;
        }

        Path tableLocation = defaultWarehouseLocation(identifier);
        try (var paths = Files.walk(tableLocation)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to drop table: " + identifier, e);
        }
    }

    Path defaultWarehouseLocation(TableIdentifier identifier) {
        Path tableLocation = warehouse;
        for (String level : identifier.namespace().levels()) {
            tableLocation = tableLocation.resolve(level);
        }
        return tableLocation.resolve(identifier.name());
    }

    private FileSystemTableOperations newTableOperations(TableIdentifier identifier) {
        return new FileSystemTableOperations(defaultWarehouseLocation(identifier));
    }
}
