DROP TABLE IF EXISTS protein_search;

CREATE TABLE protein_search (
  id TEXT NOT NULL,
  short_label TEXT,
  label TEXT NOT NULL,
  synonym TEXT,
  synonym_property TEXT
);

-- Insert ALL proteins with empty synonym value
INSERT INTO protein_search (id, short_label, label)
  SELECT s1.stanza AS id,
    s2.value AS short_label,
    s1.value AS label
  FROM statements s1
  JOIN statements s2 ON s1.stanza = s2.stanza
  WHERE s1.stanza LIKE 'uniprot:%'
    AND s1.predicate = 'rdfs:label'
    AND s2.predicate = 'CMI-PB:shortLabel';

-- Then add the rows with synonyms
INSERT INTO protein_search
  SELECT s1.stanza AS id,
    s2.value AS short_label,
    s1.value AS label,
    s3.value AS synonym,
    'IAO:0000118' AS synonym_property
  FROM statements s1
  JOIN statements s2 ON s1.stanza = s2.stanza
  JOIN statements s3 ON s1.stanza = s3.stanza
  WHERE s1.stanza LIKE 'uniprot:%'
    AND s1.predicate = 'rdfs:label'
    AND s2.predicate = 'CMI-PB:shortLabel'
    AND s3.predicate = 'IAO:0000118';
