package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

import org.jgrapht.graph.DefaultEdge;

public class LabeledEdge extends DefaultEdge {

    private final LabeledEdgeType type;

    public LabeledEdge(LabeledEdgeType type) {
        this.type = type;
    }

    public LabeledEdgeType getType() {
        return type;
    }
}
