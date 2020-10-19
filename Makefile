### Workflow
#
## [tree](build/cmi-pb-tree.html)

all: build/cmi-pb-tree.html

TABLES := upper.tsv terminology.tsv
PREFIXES := --prefixes prefixes.json
ROBOT := java -jar build/robot.jar $(PREFIXES)
ROBOT_TREE := java -jar build/robot-tree.jar $(PREFIXES)

build:
	mkdir -p $@

build/robot.jar: | build
	curl -L -o $@ https://build.obolibrary.io/job/ontodev/job/robot/job/master/lastSuccessfulBuild/artifact/bin/robot.jar

build/robot-tree.jar: | build
	curl -L -o $@ https://build.obolibrary.io/job/ontodev/job/robot/job/tree-view/lastSuccessfulBuild/artifact/bin/robot.jar

prefixes.json: prefixes.tsv
	echo '{ "@context": {' > $@
	tail -n+2 $< \
	| sed 's/\(.*\)\t\(.*\)/    "\1": "\2",/' \
	>> $@
	echo '    "CMI-PB": "http://example.com/cmi-pb/"' >> $@
	echo '} }' >> $@

cmi-pb.owl: $(TABLES) | build/robot.jar
	$(ROBOT) template \
	$(foreach T,$(TABLES),--template $(T)) \
	merge \
	--include-annotations true \
	--output $@

build/cmi-pb-tree.html: cmi-pb.owl | build/robot-tree.jar
	$(ROBOT_TREE) tree --input $< --tree $@
