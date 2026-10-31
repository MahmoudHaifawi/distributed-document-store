package com.database.atypon.app;

import com.database.atypon.Node.utils.swim.SwimBootstrap;
import com.database.atypon.Node.utils.swim.SwimConfig;
import com.database.atypon.Node.utils.swim.SwimScheduler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.config.FixedRateTask;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("node")
class NodeRoleIsolationTest {

    @Autowired
    ApplicationContext ctx;

    @Test
    void nodeBeansLoadAndGatewayBeansDoNot() {
        assertThat(ctx.getBeanNamesForType(com.database.atypon.Node.controllers.read.ReadController.class)).isNotEmpty();
        assertThat(ctx.getBeanNamesForType(com.database.atypon.DBMS.controller.DashboardController.class)).isEmpty();
    }

    @Test
    void swimJoinsAndTicksOnTheNodeRoleAlone() {
        assertThat(ctx.getBeanNamesForType(SwimBootstrap.class)).isNotEmpty();
        assertThat(ctx.getBeanNamesForType(SwimScheduler.class)).isNotEmpty();

        // @EnableScheduling on the node role registered the detector's period, read from SWIM_PERIOD_MS
        ScheduledAnnotationBeanPostProcessor scheduling = ctx.getBean(ScheduledAnnotationBeanPostProcessor.class);
        assertThat(scheduling.getScheduledTasks())
                .map(ScheduledTask::getTask)
                .filteredOn(task -> task instanceof FixedRateTask && task.getRunnable() instanceof ScheduledMethodRunnable)
                .map(task -> (FixedRateTask) task)
                .filteredOn(task -> ((ScheduledMethodRunnable) task.getRunnable()).getTarget() instanceof SwimScheduler)
                .extracting(FixedRateTask::getInterval)
                .containsExactly(SwimConfig.periodMs());
    }
}
