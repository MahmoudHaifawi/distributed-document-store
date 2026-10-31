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
                "db_write_replicas_count", "db_write_replicas_sum",
                "db_ring_size", "db_swim_members", "db_swim_probes_total", "db_swim_gossip_total",
                "db_handoff_pulls_total", "db_handoff_docs_total")) {
            List<String> samples = lines.stream().filter(l -> l.startsWith(series + "{")).collect(Collectors.toList());
            assertFalse(samples.isEmpty(), "no " + series + " sample in the scrape");
            for (String sample : samples) {
                assertTrue(sample.contains("role=\"node\""), "missing role tag: " + sample);
                assertTrue(sample.contains("node=\"TestNode\""), "missing node tag: " + sample);
            }
        }
    }

    @Test
    void theMembershipGaugesAreReadLiveOnScrapeAndAreNumbers() throws Exception {
        List<String> lines = scrapedLines();

        List<String> samples = lines.stream()
                .filter(l -> l.startsWith("db_ring_size{") || l.startsWith("db_swim_members{"))
                .collect(Collectors.toList());
        // the ring size, and one member count per state the catalogue names
        assertTrue(samples.stream().anyMatch(l -> l.startsWith("db_ring_size{")), "no db_ring_size sample");
        for (String state : List.of("alive", "suspect", "dead")) {
            assertTrue(samples.stream().anyMatch(l -> l.startsWith("db_swim_members{") && l.contains("state=\"" + state + "\"")),
                    "no db_swim_members sample for state " + state);
        }
        for (String sample : samples) {
            double value = Double.parseDouble(sample.substring(sample.lastIndexOf(' ') + 1));
            assertFalse(Double.isNaN(value), "a gauge source was lost, so it reads NaN: " + sample);
            assertTrue(value >= 0, sample);
        }
    }

    /**
     * The dashboard's p50/p95/p99 panels run {@code histogram_quantile} over {@code http_server_requests_seconds_bucket}.
     * Micrometer publishes that series only when the percentiles histogram is switched on for {@code http.server.requests};
     * without it the scrape carries just {@code _count}/{@code _sum}/{@code _max} and those panels read "No data".
     */
    @Test
    void theHttpLatencyHistogramBucketsAreScrapedForTheDashboardPercentiles() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()); // a handled request, so the timer has recorded

        List<String> buckets = scrapedLines().stream()
                .filter(l -> l.startsWith("http_server_requests_seconds_bucket{"))
                .collect(Collectors.toList());
        assertFalse(buckets.isEmpty(), "no http_server_requests_seconds_bucket series: histogram_quantile has nothing to read");
        assertTrue(buckets.stream().anyMatch(l -> l.contains("le=\"+Inf\"")), "the +Inf bucket is missing");
        assertTrue(buckets.stream().anyMatch(l -> l.matches(".*le=\"0\\.\\d+\".*")),
                "no finite sub-second latency buckets to interpolate p50/p95/p99 from");
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
