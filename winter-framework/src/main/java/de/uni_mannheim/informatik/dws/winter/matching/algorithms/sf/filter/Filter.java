package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.filter;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * Abstract SF-Filter
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public abstract class Filter<TypeA> {

    double minSim;
    boolean removeOid;

    public Filter(double minSim, boolean removeOid) {
        this.minSim = minSim;
        this.removeOid = removeOid;
    }

    public abstract List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> run(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg);

    HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> clearSfCompressed(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode<TypeA> node : ipg.vertexSet()) {
            SFNode<TypeA> nodeA = node.getPairwiseConnectivityNode().getA();
            SFNode<TypeA> nodeB = node.getPairwiseConnectivityNode().getB();
            if (node.getCurrSim() > minSim && (removeOid || nodeA.getType().equals(SFNodeType.LITERAL) && nodeB.getType().equals(SFNodeType.LITERAL))) {
                if (!nodeSimMap.containsKey(nodeA)) {
                    nodeSimMap.put(nodeA, new HashMap<>());
                }
                nodeSimMap.get(nodeA).put(nodeB, node.getCurrSim());
            }
        }
        return nodeSimMap;
    }

    HashMap<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> getClearSfAsList(HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> schema) {
        HashMap<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> schemaAsList = new HashMap<>();

        for (Entry<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> entry : schema.entrySet()) {
            List<Pair<Double, SFNode<TypeA>>> tmp = new ArrayList<>();
            for (Entry<SFNode<TypeA>, Double> entryValue : entry.getValue().entrySet()) {
                tmp.add(new Pair<>(entryValue.getValue(), entryValue.getKey()));
            }
            schemaAsList.put(entry.getKey(), tmp);
        }
        return schemaAsList;
    }

}
