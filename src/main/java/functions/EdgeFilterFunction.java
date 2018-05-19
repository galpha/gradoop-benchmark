package functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class EdgeFilterFunction implements FilterFunction<Edge> {
    @Override
    public boolean filter(Edge edge) throws Exception {
        PropertyValue value = edge.getPropertyValue("count");
        return value.getLong() > 500;
    }
}
