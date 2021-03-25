DROP TABLE IF EXISTS ab_titer_search;

CREATE TABLE ab_titer_search (
  id TEXT NOT NULL,
  short_label TEXT,
  label TEXT NOT NULL,
  synonym TEXT,
  synonym_property TEXT
);

-- Insert ALL cells with empty synonym value
INSERT INTO ab_titer_search (id, short_label, label)
  SELECT DISTINCT s1.stanza AS id,
    s2.value AS short_label,
    s3.value AS label
  FROM statements s1
  JOIN statements s2 ON s1.stanza = s2.stanza
  JOIN statements s3 ON s1.stanza = s3.stanza
  WHERE s1.predicate = 'CMI-PB:column'
  	AND s1.value = 'ab_titer.antigen'
    AND s2.predicate = 'CMI-PB:alternativeTerm'
    AND s3.predicate = 'rdfs:label';

-- Then add the rows with synonyms
INSERT INTO ab_titer_search
  SELECT DISTINCT s1.stanza AS id,
    s2.value AS short_label,
    s3.value AS label,
    s4.value AS synonym,
    s4.predicate AS synonym_property
  FROM statements s1
  JOIN statements s2 ON s1.stanza = s2.stanza
  JOIN statements s3 ON s1.stanza = s3.stanza
  JOIN statements s4 ON s1.stanza = s4.stanza
  WHERE s1.predicate = 'CMI-PB:column'
  	AND s1.value = 'ab_titer.antigen'
    AND s2.predicate = 'CMI-PB:alternativeTerm'
    AND s3.predicate = 'rdfs:label'
    AND s4.predicate IN ('oio:hasExactSynonym', 'oio:hasBroadSynonym', 'oio:hasNarrowSynonym', 'oio:hasRelatedSynonym');
