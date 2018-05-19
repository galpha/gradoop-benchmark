package functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.util.HashMap;
import java.util.Map;

public class LDBCVertrexToImportVertex implements MapFunction<LDBCVertex, ImportVertex<Long>> {

    private ImportVertex<Long> reuse;

    public LDBCVertrexToImportVertex(){
        reuse = new ImportVertex<>();
    }


    public ImportVertex<Long> map(LDBCVertex ldbcVertex) {
        reuse.setId(ldbcVertex.getVertexId());
        reuse.setLabel(ldbcVertex.getLabel());
        Map<String, Object> props = new HashMap<>();

        if (ldbcVertex.getLabel().equals("country") || ldbcVertex.getLabel().equals("tag") || ldbcVertex.getLabel().equals("city")) {
            props.put("name", ldbcVertex.getProperties().get("name"));
        }
        reuse.setProperties(Properties.createFromMap(props));
        return reuse;
    }
}
