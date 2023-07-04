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
 * Top One K as SF filter
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class TopOneK<TypeA> extends Filter<TypeA> {

    public TopOneK(double minSim, boolean removeOid) {
        super(minSim, removeOid);
    }

    @Override
    public List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> run(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        HashMap<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> schemaAsList = getClearSfAsList(clearSfCompressed(ipg));

        for (List<Pair<Double, SFNode<TypeA>>> l : schemaAsList.values()) {
            l.sort(java.util.Comparator.<Pair<Double, SFNode<TypeA>>>comparingDouble(Pair::getFirst).reversed());
        }

        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> result = new ArrayList<>();

        for (Entry<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> entry : schemaAsList.entrySet()) {
            if (entry.getValue() == null || entry.getValue().size() == 0) {
                continue;
            }
            result.add(new Pair<>(new Pair<>(entry.getKey(), entry.getValue().get(0).getSecond()), entry.getValue().get(0).getFirst()));
        }

        return result;
    }
}
