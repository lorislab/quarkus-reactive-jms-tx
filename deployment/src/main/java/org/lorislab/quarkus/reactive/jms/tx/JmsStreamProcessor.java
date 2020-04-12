package org.lorislab.quarkus.reactive.jms.tx;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

public class JmsStreamProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("lorislab-jms-tx");
    }
}
