package com.splatter0.batch.manager;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomerPartitioner implements Partitioner {

    private static final String PARTITION_KEY = "partition";

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
            var ids = (List<Integer>) context.get("ids");
            ids.add(i + 1);
        }

        return map;
    }
}
