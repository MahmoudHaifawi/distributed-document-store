package com.database.atypon.Node.services.read;

import com.database.atypon.Node.utils.PathBuilder;
import com.database.atypon.Node.utils.cache.Cache;
import com.database.atypon.Node.utils.file_operations.fileReader.FileReader;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.vclock.Reconcile;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReadService {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(ReadService.class);

    private final Cache cache;
    private final int NUMBER_OF_THREADS = 4;

    public ReadService(Cache cache) {
        this.cache = cache;
    }

    /**
     * This node's own stored copy of a document, exactly as it is on disk. That includes a tombstone: the
     * quorum read gathers these raw copies and reconciles them, and hiding a tombstone here would make a
     * replica that knows of a delete look like one that never heard of the document. The client-facing
     * answer is {@link QuorumReadService#readById}, which turns a tombstone winner into "Document not found".
     */
    public Response fetchById(String id, String databaseName, String schemaName) {
        try{
            String documentPath = PathBuilder.getPathToDocument(databaseName, schemaName, id);
            FileReader fileReader = new FileReader
                    (new File(documentPath));
            fileReader.read();
            return new Response(ResponseType.SUCCESS, "Document fetched successfully",
                    fileReader.getContent());
        }catch (Exception e){
            return new Response(ResponseType.ERROR, "Document not found");
        }
    }

    /** Every live document of the schema; deleted ones (tombstones) are left out. */
    public Response fetchAll(String databaseName, String schemaName) {
        String documentsPath = PathBuilder.getPathToAllDocuments(databaseName, schemaName);
        return readDocumentsInDirectory(new File(documentsPath));
    }

    Response readDocumentsInDirectory(File folder) {
        File[] files = folder.listFiles(File::isFile);
        if (files == null) {
            return new Response(ResponseType.ERROR, "Documents directory not found");
        }
        JSONObject result = new JSONObject();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(NUMBER_OF_THREADS, Math.max(1, files.length)));
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (File file : files) {
                tasks.add(() -> {
                    FileReader reader = new FileReader(file);
                    reader.read();
                    log.debug("Reading file {}", file.getName());
                    String content = reader.getContent();
                    if (isTombstone(content)) {
                        return null; // a deleted document is not part of what a client sees
                    }
                    synchronized (result) {
                        result.put(file.getName(), content);
                    }
                    return null;
                });
            }
            executor.invokeAll(tasks); // blocks until all reads complete
            return new Response(ResponseType.SUCCESS, "Documents fetched successfully", result.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Response(ResponseType.ERROR, "Read interrupted");
        } finally {
            executor.shutdown();
        }
    }

    private static boolean isTombstone(String content) {
        try {
            return Reconcile.isTombstone(new JSONObject(content));
        } catch (JSONException e) {
            return false; // not a document this node can interpret: leave it to the caller as before
        }
    }
}
