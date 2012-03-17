package com.marklogic.client.config.search;

/**
 * Created by IntelliJ IDEA.
 * User: ndw
 * Date: 3/14/12
 * Time: 2:32 PM
 * To change this template use File | Settings | File Templates.
 */
public interface SearchResults {
    public QueryDefinition getQueryCriteria();
    public int getTotalResults();

    public SearchMetrics          getMetrics();
    public MatchDocumentSummary[] getMatchResults();

    /*
    public FacetResult[]          getFacetResults();}
    */
}

