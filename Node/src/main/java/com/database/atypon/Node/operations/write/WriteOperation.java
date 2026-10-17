package com.database.atypon.Node.operations.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.JsonKeys;
import com.database.atypon.Node.utils.PathBuilder;
import com.database.atypon.Node.utils.Validators;
import com.database.atypon.Node.utils.file_operations.fileReader.FileReader;
import com.database.atypon.Node.utils.file_operations.fileWriter.FileWriter;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.vclock.Reconcile;
import com.database.atypon.Node.utils.vclock.VectorClock;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class WriteOperation {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WriteOperation.class);

    private final IndexManager indexManager;

    public WriteOperation(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    public Response createSchema(String database, String schemaName, JSONObject schemaDetails) {
        if (!new File(PathBuilder.getPathToDatabase(database)).exists())
            return new Response(ResponseType.ERROR, "Database doesn't exist");
        try {
            Validators.validateSchema(schemaDetails, schemaName);
            synchronized (this) {
                JSONObject finalSchema = new JSONObject();
                JSONObject infoObject = new JSONObject();

                infoObject.put(JsonKeys.SCHEMA_NAME, schemaName);
                infoObject.put(JsonKeys.NEXT_ID, 0);

                finalSchema.put(JsonKeys.INFO, infoObject);
                finalSchema.put(JsonKeys.SCHEMA, schemaDetails);

                String pathToSchema = PathBuilder.getPathToSchema(database, schemaName);
                File schemaFile = new File(pathToSchema);

                if (schemaFile.exists())
                    return new Response(ResponseType.ERROR, "Schema already exists");

                schemaFile.createNewFile();

                //create directory to the data of the schema
                File schemaData = new File(PathBuilder.getPathToAllDocuments(database, schemaName));
                try {
                    schemaData.mkdir();
                } catch (Exception e) {
                    return new Response(ResponseType.ERROR, "Error creating schema data directory");
                }
                return new FileWriter(schemaFile, finalSchema.toString()).write();
            }
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    /**
     * Origin path of a create: validates the document and stores it as version 1 with a fresh one-entry
     * vector clock (this node's own counter at 1) and the current wall-clock time. The reserved keys are
     * server-controlled, so whatever the client sent for them is replaced (or, for {@code _deleted}, dropped).
     */
    public Response createDocument(String database, String schema, JSONObject documentJSON) {
        return create(database, schema, documentJSON, false);
    }

    /**
     * Replica side of a create: like {@link #createDocument} but, when the origin already stamped the document
     * with a clock and a timestamp, keeps that stamp, so every replica of a document carries the same clock
     * instead of one clock per node (which would read as concurrent writes). A document that carries no stamp
     * is stamped here as on the origin.
     */
    public Response createReplicatedDocument(String database, String schema, JSONObject documentJSON) {
        return create(database, schema, documentJSON, true);
    }

    private Response create(String database, String schema, JSONObject documentJSON, boolean replicated) {
        try {
            Validators.validateDocument(documentJSON, schema, database);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
        synchronized (this) {
            try {
                String pathToSchema = PathBuilder.getPathToSchema(database, schema);
                File schemaFile = new File(pathToSchema);
                FileReader fileReader = new FileReader(schemaFile);
                fileReader.read();

                JSONObject schemaJSON = new JSONObject(fileReader.getContent());
                JSONObject infoObject = schemaJSON.getJSONObject(JsonKeys.INFO);
                int nextId = infoObject.getInt(JsonKeys.NEXT_ID);

                updateSchemaInfo(database, schema, nextId + 1);

                String pathToDocument = PathBuilder.getPathToDocument(database, schema,
                        String.valueOf(nextId));
                File documentFile = new File(pathToDocument);
                documentFile.createNewFile();
                documentJSON.remove(JsonKeys.DELETED); // a create never makes a tombstone
                boolean originStamped = replicated
                        && documentJSON.has(JsonKeys.VCLOCK) && documentJSON.has(JsonKeys.TS);
                if (originStamped)
                    documentJSON.put(JsonKeys.VERSION, documentJSON.optInt(JsonKeys.VERSION, 1));
                else
                    stamp(documentJSON, 1, new VectorClock()); // reserved, server-controlled
                FileWriter fileWriter = new FileWriter(documentFile, documentJSON.toString());
                fileWriter.write();
                try {
                    indexManager.onInsert(database, schema, nextId, documentJSON);
                } catch (Exception indexError) {
                    // indexes are derived/rebuildable; don't fail the write on a maintenance error
                    log.error("index maintenance failed for {}.{} doc {}", database, schema, nextId, indexError);
                }
                return new Response(ResponseType.SUCCESS, "Document created successfully");
            } catch (Exception e) {
                return new Response(ResponseType.ERROR, e.getMessage());
            }

        }
    }

    public Response updateDocument(String database, String schema, String id, JSONObject newDoc, int expectedVersion) {
        try {
            Validators.validateDocument(newDoc, schema, database);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
        synchronized (this) {
            try {
                File documentFile = new File(PathBuilder.getPathToDocument(database, schema, id));
                if (!documentFile.exists())
                    return new Response(ResponseType.ERROR, "Document not found");

                FileReader fileReader = new FileReader(documentFile);
                fileReader.read();
                JSONObject oldDoc = new JSONObject(fileReader.getContent());
                if (Reconcile.isTombstone(oldDoc))
                    return new Response(ResponseType.ERROR, "Document not found"); // deleted: it can't be updated
                int storedVersion = oldDoc.optInt(JsonKeys.VERSION, 1);
                if (storedVersion != expectedVersion)
                    return new Response(ResponseType.ERROR,
                            "Version conflict: document is at version " + storedVersion, storedVersion);

                int newVersion = storedVersion + 1;
                newDoc.remove(JsonKeys.DELETED); // an update never makes a tombstone; only a delete does
                stamp(newDoc, newVersion, VectorClock.fromJson(oldDoc.optJSONObject(JsonKeys.VCLOCK)));
                new FileWriter(documentFile, newDoc.toString()).write();
                fireOnUpdate(database, schema, Integer.parseInt(id), oldDoc, newDoc);
                return new Response(ResponseType.SUCCESS, "Document updated successfully", newVersion);
            } catch (Exception e) {
                return new Response(ResponseType.ERROR, e.getMessage());
            }
        }
    }

    /**
     * Replica side of an update: stores the copy the origin stamped, reconciled against the local one. See
     * {@link #applyReplicated}.
     */
    public Response applyUpdate(String database, String schema, String id, JSONObject doc) {
        return applyReplicated(database, schema, id, doc);
    }

    /**
     * The one causal apply behind replication: under the same lock as every other write, loads the local copy
     * (absent counts as none), reconciles it with {@code incoming} through {@link Reconcile#winner} and stores
     * the winner only when it differs from what is already here, so a copy the local one already dominates is
     * never written and applying the same copy twice changes nothing. A tombstone winner fires the delete hook
     * (handing it the live copy it replaced, so the index drops that document's keys), any other winner the
     * update hook. The response is SUCCESS either way; its content says whether the winner was applied.
     *
     * <p>This is the internal replication path for data the origin already validated, so it does not
     * schema-validate: a tombstone carries no schema fields, and the numeric round-trip inside
     * {@link Reconcile#winner} can turn an integer-valued Double into an Integer. Replicated data is trusted
     * and stored as it is.
     */
    public Response applyReplicated(String database, String schema, String id, JSONObject incoming) {
        synchronized (this) {
            try {
                File documentFile = new File(PathBuilder.getPathToDocument(database, schema, id));
                JSONObject local = null;
                if (documentFile.exists()) {
                    FileReader fileReader = new FileReader(documentFile);
                    fileReader.read();
                    local = new JSONObject(fileReader.getContent());
                }

                JSONObject winner = Reconcile.winner(local, incoming);
                if (local != null && local.similar(winner))
                    return new Response(ResponseType.SUCCESS, "Replication skipped: local copy is current", false);

                if (local == null)
                    documentFile.createNewFile();
                Response written = new FileWriter(documentFile, winner.toString()).write();
                if (written.getResponseType() == ResponseType.ERROR)
                    return written;

                int docId = Integer.parseInt(id);
                JSONObject replaced = local != null ? local : new JSONObject();
                if (Reconcile.isTombstone(winner)) {
                    fireOnDelete(database, schema, docId, replaced);
                    return new Response(ResponseType.SUCCESS, "Document deleted successfully", true);
                }
                fireOnUpdate(database, schema, docId, replaced, winner);
                return new Response(ResponseType.SUCCESS, "Document updated successfully", true);
            } catch (Exception e) {
                return new Response(ResponseType.ERROR, e.getMessage());
            }
        }
    }

    /**
     * Read-repair of one replica: stores {@code winner}, the copy a read reconciled across the replicas, by
     * the same causal rules as replication (see {@link #applyReplicated}). A winner that descends the stored
     * copy replaces it, a tombstone winner tombstones a live copy it dominates, an absent copy is healed (a
     * delete is a tombstone, so an absent file never was deleted), and a winner the stored copy already
     * dominates, or equals, is left alone, so a repair racing a newer write or a delete cannot undo either.
     * Like {@link #applyReplicated} it does not schema-validate: the winner is already-stored data, and a
     * tombstone carries no schema fields.
     */
    public Response repairDocument(String database, String schema, String id, JSONObject winner) {
        return applyReplicated(database, schema, id, winner);
    }

    private void fireOnUpdate(String database, String schema, int docId, JSONObject oldDoc, JSONObject newDoc) {
        try {
            indexManager.onUpdate(database, schema, docId, oldDoc, newDoc);
        } catch (Exception indexError) {
            // indexes are derived/rebuildable; don't fail the write on a maintenance error
            log.error("index maintenance failed for {}.{} doc {} on update", database, schema, docId, indexError);
        }
    }

    /**
     * Origin path of a delete. The file is kept and overwritten with a tombstone ({@code _deleted:true}, the
     * version bumped, this node's counter incremented in the old clock, a fresh timestamp), so the delete is a
     * causal event that replicates and reconciles like any write instead of an absence a stale replica could
     * undo. The tombstone is returned as a JSON string in the response content for the caller to replicate.
     */
    public Response deleteDocument(String database, String schema, String id, int expectedVersion) {
        synchronized (this) {
            try {
                File documentFile = new File(PathBuilder.getPathToDocument(database, schema, id));
                if (!documentFile.exists())
                    return new Response(ResponseType.ERROR, "Document not found");

                FileReader fileReader = new FileReader(documentFile);
                fileReader.read();
                JSONObject doc = new JSONObject(fileReader.getContent());
                if (Reconcile.isTombstone(doc))
                    return new Response(ResponseType.ERROR, "Document not found"); // already deleted
                int storedVersion = doc.optInt(JsonKeys.VERSION, 1);
                if (storedVersion != expectedVersion)
                    return new Response(ResponseType.ERROR,
                            "Version conflict: document is at version " + storedVersion, storedVersion);

                JSONObject tombstone = new JSONObject().put(JsonKeys.DELETED, true);
                stamp(tombstone, storedVersion + 1, VectorClock.fromJson(doc.optJSONObject(JsonKeys.VCLOCK)));
                Response written = new FileWriter(documentFile, tombstone.toString()).write();
                if (written.getResponseType() == ResponseType.ERROR)
                    return new Response(ResponseType.ERROR, "Failed to delete document");
                fireOnDelete(database, schema, Integer.parseInt(id), doc);
                return new Response(ResponseType.SUCCESS, "Document deleted successfully", tombstone.toString());
            } catch (Exception e) {
                return new Response(ResponseType.ERROR, e.getMessage());
            }
        }
    }

    /**
     * Replica side of a delete that arrives as a bare "delete this id" with no tombstone to reconcile (the
     * pre-tombstone wire form). It tombstones the local copy with a clock that descends it and applies that
     * through {@link #applyReplicated}. A copy that is absent or already deleted is left alone: there is no
     * clock to give a tombstone for a document this replica never held.
     */
    public Response applyDelete(String database, String schema, String id) {
        synchronized (this) {
            try {
                File documentFile = new File(PathBuilder.getPathToDocument(database, schema, id));
                if (!documentFile.exists())
                    return new Response(ResponseType.SUCCESS, "Document already absent");
                FileReader fileReader = new FileReader(documentFile);
                fileReader.read();
                JSONObject local = new JSONObject(fileReader.getContent());
                if (Reconcile.isTombstone(local))
                    return new Response(ResponseType.SUCCESS, "Document already deleted", false);

                JSONObject tombstone = new JSONObject().put(JsonKeys.DELETED, true);
                stamp(tombstone, local.optInt(JsonKeys.VERSION, 1) + 1,
                        VectorClock.fromJson(local.optJSONObject(JsonKeys.VCLOCK)));
                return applyReplicated(database, schema, id, tombstone);
            } catch (Exception e) {
                return new Response(ResponseType.ERROR, e.getMessage());
            }
        }
    }

    /**
     * Stamps the server-controlled keys of a write this node originates: the version, the clock (the previous
     * clock with this node's counter incremented) and the wall-clock time the concurrent-conflict tiebreak uses.
     */
    private static void stamp(JSONObject doc, int version, VectorClock previous) {
        doc.put(JsonKeys.VERSION, version);
        doc.put(JsonKeys.VCLOCK, previous.increment(selfName()).toJson());
        doc.put(JsonKeys.TS, System.currentTimeMillis());
    }

    /** The key this node owns in a vector clock: its name in the cluster, or "localhost" when it has none. */
    private static String selfName() {
        return Network.getSelf() != null ? Network.getSelf().getName() : "localhost";
    }

    private void fireOnDelete(String database, String schema, int docId, JSONObject doc) {
        try {
            indexManager.onDelete(database, schema, docId, doc);
        } catch (Exception indexError) {
            log.error("index maintenance failed for {}.{} doc {} on delete", database, schema, docId, indexError);
        }
    }

    private void updateSchemaInfo(String database, String schema, int i) throws Exception {
        try{
            String pathToSchema = PathBuilder.getPathToSchema(database, schema);
            File schemaFile = new File(pathToSchema);
            FileReader fileReader = new FileReader(schemaFile);
            fileReader.read();

            JSONObject schemaJSON = new JSONObject(fileReader.getContent());
            JSONObject infoObject = schemaJSON.getJSONObject(JsonKeys.INFO);
            infoObject.put(JsonKeys.NEXT_ID, i);

            FileWriter fileWriter = new FileWriter(schemaFile, schemaJSON.toString());
            fileWriter.write();
        }catch (Exception e){
            throw new Exception("Error updating schema info");
        }

    }
}
