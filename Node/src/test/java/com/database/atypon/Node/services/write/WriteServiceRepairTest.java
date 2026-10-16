package com.database.atypon.Node.services.write;

import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WriteServiceRepairTest {

    private final WriteOperation writeOperation = mock(WriteOperation.class);
    private final WriteService service = new WriteService(writeOperation);

    @Test
    void repairHandsTheWinnerToTheConditionalRepairNotTheVerbatimUpdate() {
        Response applied = new Response(ResponseType.SUCCESS, "Document repaired", true);
        when(writeOperation.repairDocument(eq("db"), eq("s"), eq("0"), any())).thenReturn(applied);
        HashMap<String, Object> winner = new HashMap<>();
        winner.put("Age", 30);
        winner.put("_version", 3);

        Response result = service.repairDocument("db", "s", "0", winner);

        assertThat(result).isSameAs(applied);
        ArgumentCaptor<JSONObject> passed = ArgumentCaptor.forClass(JSONObject.class);
        verify(writeOperation).repairDocument(eq("db"), eq("s"), eq("0"), passed.capture());
        assertThat(passed.getValue().getInt("Age")).isEqualTo(30);
        assertThat(passed.getValue().getInt("_version")).isEqualTo(3);
        verify(writeOperation, never()).applyUpdate(any(), any(), any(), any());
    }

    @Test
    void repairFailureComesBackAsAnErrorResponse() {
        when(writeOperation.repairDocument(any(), any(), any(), any())).thenThrow(new IllegalStateException("boom"));

        Response result = service.repairDocument("db", "s", "0", new HashMap<>());

        assertThat(result.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(result.getMessage()).isEqualTo("boom");
    }
}
