### Workflow
#
# - Open the [terminology](https://docs.google.com/spreadsheets/d/1xCrNM8Rv3v04ii1Fd8GMNTSwHzreo74t4DGsAeTsMbk/edit#gid=0) sheet
# - Run [`make all`](all) to rebuild the terminology
# - Preview the terminology [tree](./src/server/tree.sh)
# - Download the [`cmi-pb.owl`](cmi-pb.owl) file
# - Download the [`cmi-pb.db`](build/cmi-pb.db) database file

MAKEFLAGS=--warn-undefined-variables

.PHONY: all
all: build/cmi-pb.db build/predicates.txt

.PHONY: update
update:
	rm -rf build/terminology.xlsx $(TABLES)
	make all

TABLES := src/ontology/upper.tsv src/ontology/terminology.tsv build/proteins.tsv
PREFIXES := --prefixes build/prefixes.json
ROBOT := java -jar build/robot.jar $(PREFIXES)
ROBOT_TREE := java -jar build/robot-tree.jar $(PREFIXES)

build:
	mkdir -p $@

build/robot.jar: | build
	curl -L -o $@ https://build.obolibrary.io/job/ontodev/job/robot/job/master/lastSuccessfulBuild/artifact/bin/robot.jar

build/robot-tree.jar: | build
	curl -L -o $@ https://build.obolibrary.io/job/ontodev/job/robot/job/tree-view/lastSuccessfulBuild/artifact/bin/robot.jar

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	RDFTAB_URL := https://github.com/ontodev/rdftab.rs/releases/download/v0.1.1/rdftab-x86_64-apple-darwin
	JSON_SED := sed 's/\(.*\)	\(.*\)/    "\1": "\2",/'
	SQL_SED := sed 's/\(.*\)	\(.*\)/("\1", "\2"),/'
else
	RDFTAB_URL := https://github.com/ontodev/rdftab.rs/releases/download/v0.1.1/rdftab-x86_64-unknown-linux-musl
	JSON_SED := sed 's/\(.*\)\t\(.*\)/    "\1": "\2",/'
	SQL_SED := sed 's/\(.*\)\t\(.*\)/("\1", "\2"),/'
endif

build/rdftab: | build
	curl -L -o $@ $(RDFTAB_URL)
	chmod +x $@

build/terminology.xlsx: | build
	curl -L -o $@ https://docs.google.com/spreadsheets/d/1xCrNM8Rv3v04ii1Fd8GMNTSwHzreo74t4DGsAeTsMbk/export?format=xlsx

#src/ontology/%.tsv: build/terminology.xlsx
#	xlsx2csv -d tab --sheetname $* $< > $@

build/prefixes.json: src/ontology/prefixes.tsv
	echo '{ "@context": {' > $@
	tail -n+2 $< | $(JSON_SED) \
	>> $@
	echo '    "CMI-PB": "http://example.com/cmi-pb/"' >> $@
	echo '} }' >> $@

cmi-pb.owl: build/prefixes.json $(TABLES) build/imports.owl | build/robot.jar
	$(ROBOT) template \
	$(foreach T,$(TABLES),--template $(T)) \
	annotate \
	--ontology-iri "https://cmi-pb.org/terminology/cmi-pb.owl" \
	--annotation rdfs:comment "Comment" \
	--annotation dc:title "CMI-PB" \
	merge \
	--input build/imports.owl \
	--include-annotations true \
	--output $@

build/cmi-pb-tree.html: cmi-pb.owl | build/robot-tree.jar
	$(ROBOT_TREE) tree --input $< --tree $@

build/prefixes.sql: src/ontology/prefixes.tsv | build
	echo "CREATE TABLE IF NOT EXISTS prefix (" > $@
	echo "  prefix TEXT PRIMARY KEY," >> $@
	echo "  base TEXT NOT NULL" >> $@
	echo ");" >> $@
	echo "INSERT OR IGNORE INTO prefix VALUES" >> $@
	tail -n+2 $< | $(SQL_SED) \
	>> $@
	echo '("CMI-PB", "http://example.com/cmi-pb/");' >> $@

#build/cmi-pb.db: build/prefixes.sql cmi-pb.owl | build/rdftab
#	rm -f $@
#	sqlite3 $@ < $<
#	build/rdftab $@ < cmi-pb.owl
build/ontology.db: build/prefixes.sql cmi-pb.owl | build/rdftab
	rm -f $@
	sqlite3 $@ < $<
	build/rdftab $@ < cmi-pb.owl

build/ontology.sql: build/ontology.db
	sqlite3 $< '.dump "statements"' | sed 's/statements/ontology/g' > $@

.PHONY: load_ontology
load_ontology: build/cmi-pb.db build/ontology.sql
	sqlite3 $< < $(word 2,$^)


### Uniprot Proteins
# We create a ROBOT template from the Uniport RDF download for all OLink proteins

build/olink_prot_info.csv: | build
	curl -k -X 'GET' \
	 'https://www.cmi-pb.org:443/db/olink_prot_info' \
	 -H 'accept: text/csv' \
	 -H 'Range-Unit: items' \
	 > $@

build/uniprot_url.txt: build/olink_prot_info.csv
	echo "http://www.uniprot.org/uniprot/?query=" > $@.tmp
	tail -n +2 $< | awk -F ',' '{print $$1}' | tr '\n' '+' | sed 's/+/+OR+/g' | sed 's/+OR+$$//g' >> $@.tmp
	echo "&format=rdf" >> $@.tmp
	tr -d '\n' < $@.tmp > $@
	rm -rf $@.tmp

build/proteins.rdf: build/uniprot_url.txt
	$(eval URL := $(shell cat $<))
	curl -Lk "$(URL)" > $@

build/proteins.db: build/prefixes.sql build/proteins.rdf | build/rdftab
	rm -f $@
	sqlite3 $@ < $<
	build/rdftab $@ < $(word 2,$^)

build/proteins.tsv: src/build_proteins.py build/proteins.db build/olink_prot_info.csv
	python3 $^ $@


# Imports

IMPORTS := bfo chebi cl cob go obi pr vo
OWL_IMPORTS := $(foreach I,$(IMPORTS),build/$(I).owl.gz)
MODULES := $(foreach I,$(IMPORTS),build/$(I)-import.ttl)

DBS := $(foreach I,$(IMPORTS),build/$(I).db) build/cmi-pb.db build/ontology.db
dbs: $(DBS)

$(OWL_IMPORTS): | build
	curl -Lk http://purl.obolibrary.org/obo/$(subst .gz,,$(notdir $@)) | gzip > $@

build/%.db: build/%.owl.gz | build/rdftab build/prefixes.sql
	rm -rf $@
	sqlite3 $@ < build/prefixes.sql
	zcat < $< | ./build/rdftab $@

build/terms.txt: src/ontology/upper.tsv src/ontology/terminology.tsv
	cut -f1 $< \
	| tail -n+3 \
	> $@
	cut -f3 $(word 2,$^) \
	| tail -n+3 \
	| sed s!http://purl.obolibrary.org/obo/!! \
	| sed s!https://ontology.iedb.org/ontology/!! \
	| sed s!https://www.uniprot.org/uniprot/!uniprot:! \
	| sed s/_/:/ \
	>> $@

build/predicates.txt: src/ontology/upper.tsv
	grep "owl:AnnotationProperty\|rdf:Property" $< | cut -f1 > $@

ANN_PROPS := IAO:0000112 IAO:0000115 IAO:0000118 IAO:0000119 oio:hasExactSynonym oio:hasBroadSynonym oio:hasNarrowSynonym oio:hasRelatedSynonym

build/%-import.ttl: build/%.db build/terms.txt
	$(eval ANNS := $(foreach A,$(ANN_PROPS),-p $(A)))
	python3 -m gizmos.extract -d $< -T $(word 2,$^) $(ANNS) -n > $@

build/imports.owl: $(MODULES) | build/robot.jar
	$(eval INS := $(foreach M,$(MODULES), --input $(M)))
	$(ROBOT) merge $(INS) --output $@

.PHONY: clean-imports
clean-imports:
	rm -rf $(OWL_IMPORTS)

refresh-imports: clean-imports build/imports.owl


### Build Tables

### Tables from Google Sheet
GSTSV := "https://docs.google.com/spreadsheets/d/1KlG4KAuuHel8X3G3AGYraOio-8S9k7UkEdDr6avWnTM/export?format=tsv"
src/table.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=0"

src/column.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=1859463123"

src/datatype.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=1518754913"

src/prefix.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=1105305212"

src/ontology/import.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=1380652872"

src/ontology/gate.tsv: | build
	curl -L -o $@ "$(GSTSV)&gid=2143856188"

GS_TSVS := src/table.tsv src/column.tsv src/datatype.tsv src/ontology/import.tsv src/ontology/gate.tsv

.PHONY: fetch-gs-tsv
fetch-gs-tsv: $(GS_TSVS)

.PHONY: update-gs-tsv
update-gs-tsv:
	rm -f $(GS_TSVS)
	make fetch-gs-tsv

### Tables from CMI-PB API
API_TSVS := build/cell_type.tsv build/gene.tsv build/protein.tsv build/olink_prot_info.tsv build/transcript.tsv
$(API_TSVS): | build
	$(eval NAME := $(basename $(notdir $@)))
	curl -L -o $@.csv -H 'accept: text/csv' "https://www.cmi-pb.org:443/api/v2/$(NAME)"
	xsv fmt -t "	" -o $@ $@.csv
	rm $@.csv

.PHONY: fetch-api-tsv
fetch-api-tsv: $(API_TSVS)

.PHONY: update-api-tsv
update-api-tsv:
	rm -f $(API_TSVS)
	make fetch-api-tsv

### Tables from CMI-PB Download
build/2021_cmipb_challenge.zip: | build/
	curl -L -o $@ "https://www.cmi-pb.org/downloads/cmipb_challenge_datasets/2021_cmipb_challenge/2021_cmipb_challenge.zip"

build/2021_cmipb_challenge/: build/2021_cmipb_challenge.zip
	cd build && unzip -o 2021_cmipb_challenge.zip
	touch $@

ZIP_TABLES := subject specimen ab_titer live_cell_percentages olink_prot_exp rnaseq
ZIP_TSVS := $(foreach x,$(ZIP_TABLES),build/$(x).tsv)

$(ZIP_TSVS): build/2021_cmipb_challenge/
	$(eval NAME := $(basename $(notdir $@)))
	xsv cat rows $<2020LD_$(NAME).csv $<2021BD_$(NAME).csv \
	| head -n35000 \
	> $@.csv
	xsv fixlengths --output $@ $@.csv
	rm $@.csv

.PHONY: fetch-zip-tsv
fetch-zip-tsv: $(ZIP_TSVS)

.PHONY: update-zip-tsv
update-zip-tsv:
	rm -f $(ZIP_TSVS)
	make fetch-zip-tsv

.PHONY: update-tsv
update-tsv:
	rm -f $(GS_TSVS) $(API_TSVS) $(ZIP_TSVS)
	make update-gs-tsv update-api-tsv update-zip-tsv

build/cmi-pb.sql: src/script/load.py src/table.tsv src/column.tsv src/datatype.tsv src/prefix.tsv src/ontology/import.tsv src/script/validate.py | build
	python3 $< $(word 2,$^) $| > $@

# The database file we be created as a side-effect of calling src/script/load.py to create the sql file:
build/cmi-pb.db: build/cmi-pb.sql

output:
	mkdir -p $@

output/messages.tsv: src/script/export.py build/cmi-pb.db | output
	python3 $< messages $(word 2,$^) $| prefix import test_tree_under

output/%.tsv: src/script/export.py build/cmi-pb.db | output
	python3 $< data $(word 2,$^) $| $*

.PHONY: test
test: | build
	test/test.py build/cmi-pb.db

.PHONY: clean
clean:
	rm -rf build output test/output/*_actual
