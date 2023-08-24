package com.splatter0.batch.job;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.*;

public class CustomerPartitioner implements Partitioner {

    private static final String PARTITION_KEY = "partition";

    @SuppressWarnings("NullableProblems")
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        var map = new HashMap<String, ExecutionContext>();
        for (int i = 0; i < gridSize; i++) {
            var context = new ExecutionContext();
            context.put("ids", new ArrayList<Integer>());
            map.put(PARTITION_KEY + i, context);
        }
        for (int i = 0; i < 100; i++) {
            var key = PARTITION_KEY + (i % gridSize);
            var context = map.get(key);
            //noinspection unchecked
            var ids = (List<Integer>) context.get("ids");
            Objects.requireNonNull(ids).add(i + 1);
        }

        return map;
    }
}
