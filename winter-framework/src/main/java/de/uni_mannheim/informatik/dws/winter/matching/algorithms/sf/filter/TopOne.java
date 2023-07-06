package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.filter;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * TopOne as SF filter
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class TopOne<TypeA> extends Filter<TypeA> {

    public TopOne(double minSim, boolean removeOid) {
        super(minSim, removeOid);
    }

    @Override
    public List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> run(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> schema = clearSfCompressed(ipg);

        List<Pair<SFNode<TypeA>, Pair<SFNode<TypeA>, Double>>> sortedFlatList = new ArrayList<>();
        for (Entry<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> entry : schema.entrySet()) {
            SFNode<TypeA> nodeA = entry.getKey();
            for (Entry<SFNode<TypeA>, Double> entryValue : entry.getValue().entrySet()) {
                SFNode<TypeA> nodeB = entryValue.getKey();
                double sim = entryValue.getValue();
                sortedFlatList.add(new Pair<>(nodeA, new Pair<>(nodeB, sim)));
            }
        }

        sortedFlatList.sort(java.util.Comparator.<Pair<SFNode<TypeA>, Pair<SFNode<TypeA>, Double>>>comparingDouble(x -> x.getSecond().getSecond()).reversed());

        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> result = new ArrayList<>();
        List<Pair<SFNode<TypeA>, Pair<SFNode<TypeA>, Double>>> sortedFlatListCopy = new ArrayList<>(sortedFlatList);
        for (Pair<SFNode<TypeA>, Pair<SFNode<TypeA>, Double>> list : sortedFlatListCopy) {
            HashMap<SFNode<TypeA>, Double> entry = schema.get(list.getFirst());

            SFNode<TypeA> nodeA = list.getFirst();
            SFNode<TypeA> nodeB = null;
            if (entry == null || entry.values().size() == 0) {
                continue;
            }

            for (Pair<SFNode<TypeA>, Pair<SFNode<TypeA>, Double>> pair : sortedFlatList) {
                if (pair.getFirst().equals(nodeA)) {
                    nodeB = pair.getSecond().getFirst();
                    double sim = pair.getSecond().getSecond();
                    result.add(new Pair<>(new Pair<>(nodeA, nodeB), sim));
                    break;
                }
            }

            sortedFlatList.removeIf(x -> x.getFirst().equals(nodeA));
            SFNode<TypeA> finalNodeB = nodeB;
            sortedFlatList.removeIf(x -> x.getSecond().getFirst().equals(finalNodeB));
        }

        return result;
    }
}
