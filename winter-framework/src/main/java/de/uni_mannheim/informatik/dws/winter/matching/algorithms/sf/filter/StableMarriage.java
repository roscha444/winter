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
 * Stable Marriage Algorithm as SF filter
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class StableMarriage<TypeA> extends Filter<TypeA> {

    public StableMarriage(double minSim, boolean removeOid) {
        super(minSim, removeOid);
    }

    @Override
    public List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> run(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> engagements = new ArrayList<>();

        HashMap<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> schema1Engagements = new HashMap<>();
        HashMap<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> schema2Engagements = new HashMap<>();
        initEngagementsSchemas(ipg, schema1Engagements, schema2Engagements);

        boolean change = true;
        while (change) {
            change = false;

            for (Entry<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> entry : schema1Engagements.entrySet()) {
                if (entry.getValue().getFirst() == null && entry.getValue().getSecond().size() > 0) {
                    List<Pair<Double, SFNode<TypeA>>> possibleMarriages = entry.getValue().getSecond();
                    Pair<Double, SFNode<TypeA>> proposal = possibleMarriages.remove(0);

                    if (schema2Engagements.get(proposal.getSecond()) == null) {
                        schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                        entry.setValue(new Pair<>(proposal, entry.getValue().getSecond()));
                    } else {
                        if (schema2Engagements.get(proposal.getSecond()).getFirst() < proposal.getFirst()) {
                            Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>> oldPair = schema1Engagements.get(schema2Engagements.get(proposal.getSecond()).getSecond());
                            schema1Engagements.put(schema2Engagements.get(proposal.getSecond()).getSecond(), new Pair<>(null, oldPair.getSecond()));
                            schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                            entry.setValue(new Pair<>(proposal, entry.getValue().getSecond()));
                        }
                    }
                    change = true;
                }
            }
        }

        for (Entry<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> entry : schema1Engagements.entrySet()) {
            SFNode<TypeA> node = entry.getKey();
            Pair<Double, SFNode<TypeA>> bestMatch = entry.getValue().getFirst();
            double bestMatchSim = 0.0;
            SFNode<TypeA> bestMatchNode = null;
            if (bestMatch != null) {
                bestMatchSim = bestMatch.getFirst();
                bestMatchNode = bestMatch.getSecond();
            }
            engagements.add(new Pair<>(new Pair<>(node, bestMatchNode), bestMatchSim));
        }

        for (Entry<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> entry : schema2Engagements.entrySet()) {
            if (entry.getValue() == null) {
                engagements.add(new Pair<>(new Pair<>(null, entry.getKey()), 0.0));
            }
        }

        return engagements;
    }

    private void initEngagementsSchemas(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg,
        HashMap<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> schema1Engagements,
        HashMap<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> schema2Engagements) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> schema = clearSfCompressed(ipg);

        for (Entry<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> nodeAEntry : schema.entrySet()) {
            SFNode<TypeA> nodeA = nodeAEntry.getKey();

            for (Entry<SFNode<TypeA>, Double> nodeBEntry : nodeAEntry.getValue().entrySet()) {
                SFNode<TypeA> nodeB = nodeBEntry.getKey();

                if (!schema1Engagements.containsKey(nodeA)) {
                    schema1Engagements.put(nodeA, new Pair<>(null, new ArrayList<>()));
                }
                schema1Engagements.get(nodeA).getSecond().add(new Pair<>(schema.get(nodeA).get(nodeB), nodeB));
                if (!schema2Engagements.containsKey(nodeB)) {
                    schema2Engagements.put(nodeB, null);
                }
            }
        }

        for (Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>> l : schema1Engagements.values()) {
            l.getSecond().sort(java.util.Comparator.<Pair<Double, SFNode<TypeA>>>comparingDouble(Pair::getFirst).reversed());
        }
    }
}
