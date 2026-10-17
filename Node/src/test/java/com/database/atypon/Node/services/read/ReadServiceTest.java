package com.database.atypon.Node.services.read;

import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class ReadServiceTest {

    // Cache is not exercised by the read paths under test (document caching lands in M1),
    // so a null cache keeps this a focused unit test, independent of the Cache task's ordering.
    private final ReadService service = new ReadService(null);

    @Test
    void readsAllDocumentsInADirectory(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("0.json"), new JSONObject().put("Name", "A").toString());
        Files.writeString(dir.resolve("1.json"), new JSONObject().put("Name", "B").toString());

        Response response = service.readDocumentsInDirectory(dir.toFile());

        assertThat(response.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(response.getContent().toString()).contains("0.json").contains("1.json");
    }

    @Test
    void readingAllDocumentsOmitsTombstones(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("0.json"), new JSONObject().put("Name", "A").toString());
        Files.writeString(dir.resolve("1.json"), new JSONObject().put("_deleted", true).put("_version", 2)
                .put("_vclock", new JSONObject().put("n1", 2)).toString());
        Files.writeString(dir.resolve("2.json"), new JSONObject().put("Name", "C").put("_deleted", false).toString());

        Response response = service.readDocumentsInDirectory(dir.toFile());

        assertThat(response.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject all = new JSONObject(response.getContent().toString());
        assertThat(all.keySet()).containsExactlyInAnyOrder("0.json", "2.json");
    }

    @Test
    void concurrentReadsBothSucceed(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("0.json"), new JSONObject().put("Name", "A").toString());
        File folder = dir.toFile();

        CompletableFuture<Response> a = CompletableFuture.supplyAsync(() -> service.readDocumentsInDirectory(folder));
        CompletableFuture<Response> b = CompletableFuture.supplyAsync(() -> service.readDocumentsInDirectory(folder));

        assertThat(a.get().getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(b.get().getResponseType()).isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void missingDirectoryReturnsErrorNotNull() {
        Response response = service.readDocumentsInDirectory(new File("does-not-exist-xyz"));
        assertThat(response).isNotNull();
        assertThat(response.getResponseType()).isEqualTo(ResponseType.ERROR);
    }
}
