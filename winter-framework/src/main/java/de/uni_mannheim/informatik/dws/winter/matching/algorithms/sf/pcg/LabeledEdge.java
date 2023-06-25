package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg;

import org.jgrapht.graph.DefaultEdge;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
public class LabeledEdge extends DefaultEdge {

    private final LabeledEdgeType type;

    public LabeledEdge(LabeledEdgeType type) {
        this.type = type;
    }

    public LabeledEdgeType getType() {
        return type;
    }
}
