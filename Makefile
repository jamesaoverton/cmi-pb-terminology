### Workflow
#
# - [terminology](https://docs.google.com/spreadsheets/d/1xCrNM8Rv3v04ii1Fd8GMNTSwHzreo74t4DGsAeTsMbk/edit#gid=0)
# - [all](all)
# - [tree](./src/server/tree.sh)
# - [db](build/cmi-pb.db)
# - [owl](cmi-pb.owl)


.PHONY: all
all: build/cmi-pb.db build/predicates.txt

.PHONY: update
update:
	rm -rf build/terminology.xlsx $(TABLES)
	make all

TABLES := src/ontology/upper.tsv src/ontology/terminology.tsv
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
else
	RDFTAB_URL := https://github.com/ontodev/rdftab.rs/releases/download/v0.1.1/rdftab-x86_64-unknown-linux-musl
endif

build/rdftab: | build
	curl -L -o $@ $(RDFTAB_URL)
	chmod +x $@

build/terminology.xlsx: | build
	curl -L -o $@ https://docs.google.com/spreadsheets/d/1xCrNM8Rv3v04ii1Fd8GMNTSwHzreo74t4DGsAeTsMbk/export?format=xlsx

src/ontology/%.tsv: build/terminology.xlsx
	xlsx2csv -d tab --sheetname $* $< > $@

build/prefixes.json: src/ontology/prefixes.tsv
	echo '{ "@context": {' > $@
	tail -n+2 $< \
	| sed 's/\(.*\)\t\(.*\)/    "\1": "\2",/' \
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

build/prefixes.sql: build/prefixes.tsv | build
	echo "CREATE TABLE IF NOT EXISTS prefix (" > $@
	echo "  prefix TEXT PRIMARY KEY," >> $@
	echo "  base TEXT NOT NULL" >> $@
	echo ");" >> $@
	echo "INSERT OR IGNORE INTO prefix VALUES" >> $@
	tail -n+2 $< \
	| sed 's/\(.*\)\t\(.*\)/("\1", "\2"),/' \
	>> $@
	echo '("CMI-PB", "http://example.com/cmi-pb/");' >> $@

build/cmi-pb.db: build/prefixes.sql cmi-pb.owl | build/rdftab
	rm -f $@
	sqlite3 $@ < $<
	build/rdftab $@ < cmi-pb.owl


# Imports

IMPORTS := bfo chebi cl cob go obi pr vo
OWL_IMPORTS := $(foreach I,$(IMPORTS),build/$(I).owl)
DBS := build/cmi-pb.db $(foreach I,$(IMPORTS),build/$(I).db)
MODULES := $(foreach I,$(IMPORTS),build/$(I)-import.ttl)

dbs: $(DBS)

$(OWL_IMPORTS): | build
	curl -Lk -o $@ http://purl.obolibrary.org/obo/$(notdir $@)

build/%.db: build/%.owl | build/rdftab build/prefixes.sql
	rm -rf $@
	sqlite3 $@ < build/prefixes.sql
	./build/rdftab $@ < $<

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

ANN_PROPS := IAO:0000112 IAO:0000115 IAO:0000118 IAO:0000119

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
