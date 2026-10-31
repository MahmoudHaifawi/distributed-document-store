package com.database.atypon.Node.controllers;

import com.database.atypon.Node.utils.PathBuilder;
import com.database.atypon.Node.utils.file_operations.fileReader.FileReader;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The serving end of a state handoff (see {@code HandoffService}). Node-to-node only: {@code /swim/**}
 * requires the INTERNAL role (see {@code SecurityConfig}).
 *
 * <p>Both endpoints read the data directory directly. In particular {@code /swim/pull} does not go through
 * {@code ReadService.fetchAll}, which leaves tombstones out because a client must not see deleted documents:
 * a handoff has to carry the deletes along with the live documents, or the node receiving them would take
 * "this document is gone" for "this document never existed" and let a stale replica bring it back.
 */
@RestController
public class HandoffController {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HandoffController.class);

    /** Keys of the {@code /swim/pull} answer. */
    public static final String DEFINITION = "definition";
    public static final String DOCUMENTS = "documents";

    private static final String JSON_SUFFIX = ".json";
    private static final Pattern DOCUMENT_FILE = Pattern.compile("\\d+\\.json");

    /**
     * Every schema this node holds, as {@code database.schema} keys (the same key the ring places a schema
     * by), sorted. A schema is held when both its definition and its data directory are here.
     */
    @GetMapping(value = "/swim/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> schemas() {
        List<String> keys = new ArrayList<>();
        File[] databases = new File(PathBuilder.getRootPath()).listFiles(File::isDirectory);
        if (databases == null) {
            return keys;
        }
        for (File database : databases) {
            String databaseName = database.getName();
            File[] definitions = new File(PathBuilder.getPathToAllSchemas(databaseName))
                    .listFiles(file -> file.isFile() && file.getName().endsWith(JSON_SUFFIX));
            if (definitions == null) {
                continue;
            }
            for (File definition : definitions) {
                String name = definition.getName();
                String schemaName = name.substring(0, name.length() - JSON_SUFFIX.length());
                if (new File(PathBuilder.getPathToAllDocuments(databaseName, schemaName)).isDirectory()) {
                    keys.add(databaseName + "." + schemaName);
                }
            }
        }
        keys.sort(Comparator.naturalOrder());
        return keys;
    }

    /**
     * One schema, whole: its definition (the schema file) and every document file of it as
     * {@code {"<id>": <document>}}, each exactly as stored, a tombstone included. Not found if this node does
     * not hold the schema (no data directory, or no definition).
     *
     * <p>The answer is all or nothing. This reads the files without taking any write lock, and a writer
     * truncates a file before it rewrites it (every create rewrites the schema file), so a read can land on a
     * file that is empty or half written. Leaving that file out, or sending the definition as {@code null},
     * would hand the receiver a snapshot that looks complete and is not: it would store the documents, decide
     * the schema had been handed over, and never learn the definition. So if the definition, or any file named
     * like a document ({@code <digits>.json}), cannot be read as JSON, the pull fails with 503 and the
     * receiver tries another replica, or this one at the next change. Files not named like a document are
     * ignored.
     */
    @GetMapping(value = "/swim/pull", produces = MediaType.APPLICATION_JSON_VALUE)
    public String pull(@RequestParam String database, @RequestParam String schema) {
        if (!isSafeName(database) || !isSafeName(schema)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "database and schema must be plain names");
        }
        File directory = new File(PathBuilder.getPathToAllDocuments(database, schema));
        File[] files = directory.listFiles(file -> file.isFile() && DOCUMENT_FILE.matcher(file.getName()).matches());
        if (files == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "this node does not hold " + database + "." + schema);
        }
        File definitionFile = new File(PathBuilder.getPathToSchema(database, schema));
        if (!definitionFile.isFile()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "this node does not hold " + database + "." + schema);
        }
        Arrays.sort(files, Comparator.comparing(File::getName));

        JSONObject definition = readJson(definitionFile);
        if (definition == null) {
            throw unreadable(definitionFile);
        }
        JSONObject documents = new JSONObject();
        for (File file : files) {
            JSONObject document = readJson(file);
            if (document == null) {
                throw unreadable(file);
            }
            String name = file.getName();
            documents.put(name.substring(0, name.length() - JSON_SUFFIX.length()), document);
        }
        return new JSONObject()
                .put("database", database)
                .put("schema", schema)
                .put(DEFINITION, definition)
                .put(DOCUMENTS, documents)
                .toString();
    }

    private static ResponseStatusException unreadable(File file) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "cannot read " + file.getName() + " right now; try again");
    }

    /**
     * Whether {@code name} can safely be a database or schema directory name: not empty, and nothing that
     * could step out of the data directory. The callers are trusted nodes; this is for a bug or a bad peer,
     * not for an attacker.
     */
    public static boolean isSafeName(String name) {
        return name != null && !name.isBlank() && !name.equals(".") && !name.equals("..")
                && name.indexOf('/') < 0 && name.indexOf('\\') < 0 && name.indexOf('\0') < 0;
    }

    /** The file as JSON, or null if it cannot be read or parsed (typically: a writer is in the middle of it). */
    private static JSONObject readJson(File file) {
        try {
            FileReader reader = new FileReader(file);
            reader.read();
            return new JSONObject(reader.getContent());
        } catch (RuntimeException e) {
            log.warn("handoff: {} cannot be read as JSON: {}", file.getPath(), e.toString());
            return null;
        }
    }
}
