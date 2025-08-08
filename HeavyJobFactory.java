package com.digiplus.social.operation.heavyjob;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 重型任務實例工廠
 */
@Slf4j
@Component
public class HeavyJobFactory {

    private final Map<String, HeavyJob> heavyJobMap = new HashMap<>();

    @Autowired
    public void init(List<HeavyJob> heavyJobs) {
        heavyJobs.forEach(heavyJob -> {
            String className = heavyJob.getClass().getSimpleName();
            heavyJobMap.put(className, heavyJob);
        });
    }

    public HeavyJob getHeavyJob(String className) {
        HeavyJob heavyJob = heavyJobMap.get(className);
        if (heavyJob == null) {
            log.error("Heavy job not found: {}", className);
            throw new IllegalArgumentException("Heavy job not found: " + className);
        }
        return heavyJob;
    }
}
