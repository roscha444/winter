package de.uni_mannheim.informatik.dws.winter.usecase.events.identityresolution;


import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.usecase.events.model.Event;
import de.uni_mannheim.informatik.dws.winter.usecase.events.utils.BestListSimilarity;

/**
 * {@link Comparator} for {@link Event}s based on the striped {@link Event#getUris()}
 * value and their {@link TokenizingJaccardSimilarity} value.
 *
 * @author Daniel Ringler
 *
 */
public class EventURIComparatorJaccard implements Comparator<Event, Attribute> {

    private static final long serialVersionUID = 1L;
    private BestListSimilarity bestListSimilarity = new BestListSimilarity();
    private TokenizingJaccardSimilarity sim = new TokenizingJaccardSimilarity();
    
    private ComparatorLogger comparisonLog;

    @Override
    public double compare(
            Event record1,
            Event record2,
            Correspondence<Attribute, Matchable> schemaCorrespondences) {
    	
    	double similarity = bestListSimilarity.getBestStripedStringSimilarity(sim, record1.getUris(), record2.getUris());
    	
    	if(this.comparisonLog != null){
    		this.comparisonLog.setComparatorName(getClass().getName());
    	
    		this.comparisonLog.setRecord1Value(record1.getUris().toString());
    		this.comparisonLog.setRecord2Value(record2.getUris().toString());
		
    		this.comparisonLog.setSimilarity(Double.toString(similarity));
    	}
    	
        return similarity;
        
    }

	@Override
	public ComparatorLogger getComparisonLog() {
		return this.comparisonLog;
	}

	@Override
	public void setComparisonLog(ComparatorLogger comparatorLog) {
		this.comparisonLog = comparatorLog;
	}
}
