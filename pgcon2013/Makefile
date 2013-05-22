DOCUMENT = pg-progress

PYGMENTIZE_TEX = pygmentize.tex
PYGMENTIZE = pygmentize

EXTRA_CLEAN = $(wildcard *.toc *.aux *.pdf *.log \
	*.out *.nav *.snm *.vrb *.bbl *.blg) $(PYGMENTIZE_TEX) \
	$(DIAGRAM_PDFS)

DIAGRAM_DOTS = $(wildcard *.dot)
DIAGRAM_PDFS = $(patsubst %.dot,%.pdf,$(DIAGRAM_DOTS))

.SECONDARY: $(PYGMENTIZE_TEX) $(DIAGRAM_PDFS)

all: $(DOCUMENT).pdf

clean:
	rm -f $(EXTRA_CLEAN)

$(PYGMENTIZE_TEX):
	echo | $(PYGMENTIZE) -l python -f latex -O full | sed -n \
		'/makeatletter/,/makeatother/p' > $@
%.pdf: %.dot
	dot -Tpdf -o $@ $<

%.pdf: %.tex $(PYGMENTIZE_TEX) $(DIAGRAM_PDFS)
	pdflatex $<
	pdflatex $<
