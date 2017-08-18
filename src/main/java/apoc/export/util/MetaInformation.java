package apoc.export.util;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.parboiled.common.ImmutableList;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 19.01.14
 */
public class MetaInformation {

    public static Map<String,Class> collectPropTypesForNodes(SubGraph graph) {
        Map<String,Class> propTypes = new LinkedHashMap<>();
        for (Node node : graph.getNodes()) {
            updateKeyTypes(propTypes, node);
        }
        return propTypes;
    }
    public static Map<String,Class> collectPropTypesForRelationships(SubGraph graph) {
        Map<String,Class> propTypes = new LinkedHashMap<>();
        for (Relationship node : graph.getRelationships()) {
            updateKeyTypes(propTypes, node);
        }
        return propTypes;
    }

    public static void updateKeyTypes(Map<String, Class> keyTypes, PropertyContainer pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Class storedClass = keyTypes.get(prop);
            if (storedClass==null) {
                keyTypes.put(prop,value.getClass());
                continue;
            }
            if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
            if (value instanceof Number) {
                Class<? extends Number> type = NUMBER_TYPES.stream()
                        .filter(n -> n == storedClass || n == value.getClass())
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(value.getClass().getName()));
                keyTypes.put(prop, type);
                continue;
            }
            keyTypes.put(prop, void.class);
        }
    }

    @SuppressWarnings("unchecked")
    private static final ImmutableList<Class<? extends Number>> NUMBER_TYPES = ImmutableList.of(
            Long.class, Double.class, Integer.class, Float.class, Short.class, Byte.class
    );

    public final static Set<String> GRAPHML_ALLOWED = new HashSet<>(asList("boolean", "int", "long", "float", "double", "string"));

    public static String typeFor(Class value, Set<String> allowed) {
        if (value == void.class) return null;
        if (value.isArray()) return typeFor(value.getComponentType(),allowed);
        String name = value.getSimpleName().toLowerCase();
        if (name.equals("integer")) name="int";
        if (allowed==null || allowed.contains(name)) return name;
        if (Number.class.isAssignableFrom(value)) return "int";
        return null;
    }

    public static String getLabelsString(Node node) {
        if (!node.getLabels().iterator().hasNext()) return "";
        String delimiter = ":";
        return ":" + FormatUtils.joinLabels(node, delimiter);
    }

}
