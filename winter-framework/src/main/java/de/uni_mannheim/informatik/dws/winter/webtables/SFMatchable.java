package de.uni_mannheim.informatik.dws.winter.webtables;

import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;

/**
 * Model of a Web Table column.
 *
 * @author Oliver Lehmberg (oli@dwslab.de)
 */
public abstract class SFMatchable implements Matchable {

    protected DataType type;

    public SFMatchable(DataType type) {
        this.type = type;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }
}