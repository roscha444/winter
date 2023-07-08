package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg;

import java.util.Objects;

/**
 * This class represents a node in the graph representation for a relational schema according to the SF-algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFNode<TypeA> {

    private String getIdentifier;
    private final SFNodeType type;
    private TypeA matchable;

    public SFNode(String getIdentifier, SFNodeType type) {
        this.getIdentifier = getIdentifier;
        this.type = type;
        this.matchable = null;
    }

    public SFNode(String getIdentifier, SFNodeType type, TypeA matchable) {
        this.getIdentifier = getIdentifier;
        this.type = type;
        this.matchable = matchable;
    }

    public TypeA getMatchable() {
        return matchable;
    }

    public void setMatchable(TypeA matchable) {
        this.matchable = matchable;
    }

    public SFNodeType getType() {
        return type;
    }

    public String getGetIdentifier() {
        return getIdentifier;
    }

    public void setGetIdentifier(String getIdentifier) {
        this.getIdentifier = getIdentifier;
    }

    @Override
    public String toString() {
        return "SFNode{" +
            "getIdentifier='" + getIdentifier + '\'' +
            ", type=" + type +
            ", matchable=" + matchable +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SFNode<?> sfNode = (SFNode<?>) o;
        return Objects.equals(getIdentifier, sfNode.getIdentifier) && type == sfNode.type && Objects.equals(matchable, sfNode.matchable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier, type, matchable);
    }
}
