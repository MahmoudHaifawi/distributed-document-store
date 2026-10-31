package com.database.atypon.app;

import com.database.atypon.Node.metrics.DbMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * M12 observability: the Node module's domain meters land in the application's Prometheus registry, not in the
 * private fallback {@link DbMetrics} uses when the context has no registry, and are scraped under the exact names
 * the Grafana dashboard queries, carrying the {@code role} and {@code node} common tags.
 */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics // @SpringBootTest switches metrics export off by default, which would drop the Prometheus registry
@ActiveProfiles("node")
@TestPropertySource(properties = {
        "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000",
        "NODE_NAME=TestNode"})
class DbMetricsExposureTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    DbMetrics dbMetrics;
    @Autowired
    MeterRegistry registry;

    private List<String> scrapedLines() throws Exception {
        String body = mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return Arrays.asList(body.split("\n"));
    }

    @Test
    void theDomainMetersUseTheApplicationRegistry() {
        assertSame(registry, dbMetrics.registry(), "DbMetrics fell back to a private registry nobody scrapes");
    }

    @Test
    void theCatalogueMetersAreScrapedByNameWithTheCommonTags() throws Exception {
        List<String> lines = scrapedLines();

        for (String series : List.of("db_write_total", "db_read_total", "db_read_repair_total",
                "db_read_repair_docs_total", "db_reconcile_conflicts_total", "db_tombstone_written_total",
                "db_write_replicas_count", "db_write_replicas_sum")) {
            List<String> samples = lines.stream().filter(l -> l.startsWith(series + "{")).collect(Collectors.toList());
            assertFalse(samples.isEmpty(), "no " + series + " sample in the scrape");
            for (String sample : samples) {
                assertTrue(sample.contains("role=\"node\""), "missing role tag: " + sample);
                assertTrue(sample.contains("node=\"TestNode\""), "missing node tag: " + sample);
            }
        }
    }

    @Test
    void aRecordedEventShowsUpInTheScrape() throws Exception {
        dbMetrics.recordWrite(DbMetrics.WriteResult.QUORUM_FAILED, DbMetrics.WriteType.UPDATE,
                DbMetrics.WriteOrigin.FORWARDED);

        String sample = scrapedLines().stream()
                .filter(l -> l.startsWith("db_write_total{") && l.contains("result=\"quorum_failed\"")
                        && l.contains("type=\"update\"") && l.contains("origin=\"forwarded\""))
                .findFirst().orElseThrow(() -> new AssertionError("db_write_total{quorum_failed,update,forwarded} not scraped"));
        assertTrue(Double.parseDouble(sample.substring(sample.lastIndexOf(' ') + 1)) >= 1.0, sample);
    }
}
