package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.security.JwtService;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.HashMap;
import java.util.Vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WriteControllerRepairTest {

    private final WriteService writeService = mock(WriteService.class);
    private final WriteController controller = new WriteController(writeService);

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void repairAppliesTheWinnerConditionallyAndNeverRunsAnOriginWrite() {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("node", "token", JwtService.authorities("INTERNAL")));
        Response applied = new Response(ResponseType.SUCCESS, "Document repaired", true);
        when(writeService.repairDocument(eq("db"), eq("s"), eq("0"), any())).thenReturn(applied);
        HashMap<String, Object> winner = new HashMap<>();
        winner.put("_version", 3);

        Vector<Response> out = controller.repairDocument("db", "s", "0", winner);

        assertThat(out).containsExactly(applied);
        verify(writeService).repairDocument("db", "s", "0", winner);
        verify(writeService, never()).applyUpdate(any(), any(), any(), any());
        verify(writeService, never()).updateDocument(any(), any(), any(), any(), anyInt());
        verify(writeService, never()).broadcastUpdate(any(), any(), any(), any());
    }
}
