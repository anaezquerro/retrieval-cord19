# Welcome to the `retrieval-cord19` repository!  <img class=" lazyloaded" src="emojis\Magnifying Glass Tilted Left.png" width="31" height="31">

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Structure of the repository](#structure-of-the-repository)
4. [Implemented models](#implemented-models)
5. [Results](#results)

## Introduction

This repo implements with Java and [Apache Lucene](https://lucene.apache.org/) the classical information retrieval models in the literature. This project is an 
extension of a college assignment for a subject named "_Information Retrieval_". The models implemented 
are those explained in the popular Manning's book [_Introduction to Information Retrieval_](https://nlp.stanford.edu/IR-book/), 
openly available in the Stanford University website.

These classical methods are tested with the [CORD19 dataset](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7251955/) (using the _2020-07-16_ release) 
following the [TREC-COVID Challenge](https://ir.nist.gov/trec-covid/) scheme, where a 
[topics set](https://ir.nist.gov/trec-covid/data/topics-rnd5.xml) is used in order to launch queries to the retrieval 
model and a set of [relevance judgements](https://ir.nist.gov/trec-covid/data/qrels-covid_d5_j0.5-5.txt) is used to 
compute evaluation metrics.

In order to tackle the retrieval problem, our implementation considers:

- The `models` field of each topic (see the XML file of the [topics set](2020-07-16/topics-set.xml)).

```xml
<topics task="COVIDSearch 2020" batch="5">
    <topic number="1">
    <models>coronavirus origin</models>
    <question>what is the origin of COVID-19</question>
    <narrative>seeking range of information about the SARS-CoV-2 virus's origin, including its evolution, animal source, and first transmission into humans</narrative>
    </topic>
    <topic number="2">
    <models>coronavirus response to weather changes</models>
    <question>how does the coronavirus respond to changes in the weather</question>
    <narrative>seeking range of information about the SARS-CoV-2 virus viability in different weather/climate conditions as well as information related to transmission of the virus in different climate conditions</narrative>
    </topic>
...
</topics>
```
- The title, abstract, authors, body and references of each article in the [CORD19 dataset](2020-07-16).
- The [document embeddings](2020-07-16/embeddings.csv) provided with the collection. 

_Note_: If you are unfamiliar with the [TREC-COVID Challenge](https://ir.nist.gov/trec-covid/), consider reading:

- Information about the structure of the [metadata CSV file](2020-07-16/metadata.csv), [JSON article files](2020-07-16/document_parses), 
and more in the [official GitHub repo of the CORD19 dataset](https://github.com/allenai/cord19).
- Information about the scheme of the [relevance judgements TXT file](2020-07-16/relevance-judgements.txt) in the 
[TREC-COVID Challenge page](https://ir.nist.gov/trec-covid/qrels5.html).

## Installation

To execute this code (Java and Python files), the following prerequisites are needed:

- [Java 17](https://www.oracle.com/java/technologies/downloads/#java17).
- [Python 3.10](https://www.python.org/downloads/release/python-3100/) and [pip](https://pip.pypa.io/en/stable/installation/). 
Once both have been installed, install [wget](https://pypi.org/project/wget/), [tarfile](https://docs.python.org/3/library/tarfile.html), 
[bs4](https://pypi.org/project/bs4/) and [transformers](https://huggingface.co/transformers) libraries by running in terminal:
```shell
pip install wget tarfile bs4 transformers
```
- To download the CORD19 dataset and TREC-COVID Collection files, run `download-data.py` file in `retrieval-cord19/`  via:
```shell
python download-data.py
```

To contribute to this repo or experiment with the modules, we provide the [pom](pom.xml) file to automatically create the 
[Apache Maven](https://maven.apache.org/install.html) project.


## Structure of the repository

Once data has been download in the previous section, in order to understand and execute the Java code of this repository, the following structure is needed:

```
retrieval-cord19/
    2020-07-16/
        document_parses/
            pdf_json/
            pmc_json/
            embeddings.csv
            metadata.csv
            relevance-judgements.txt
            topics-embeddings.json
            topics-set.xml
    src/
      cords/
      formats/
      lucene/
      models/
      schemas/
      util/
```

The folder [`2020-07-16/`](2020-07-16) contains the CORD19 dataset along with TREC-COVID auxiliary files: 
  - [`document_parses/`](2020-07-16/document_parses/) contains the PMC and PDF articles in JSON format.
  - [`metadata.csv`](2020-07-16/metadata.csv) contains, by row, the most important information about each article of the 
  CORD19 dataset.
  - [`embeddings.csv`](2020-07-16/embeddings.csv) contains, by row, the article embedding computed by a pretrained
  [SPECTER](https://github.com/allenai/specter). 
  - [`relevance-judgements.txt`](2020-07-16/relevance-judgements.txt) contains, by row, the relevance judgements of the TREC-COVID Collection.
  - [`topics-set.xml`](2020-07-16/topics-set.xml) contains the TREC-COVID topics info.
  - [`topics-embeddings.json`](2020-07-16/topics-embeddings.json) contains the embeddings of each topic query and narrative 
  field computed with [SPECTER](https://github.com/allenai/specter).

The folder [`src/`](src) stores Java packages that implement the reading, parsing, 
indexing and querying processes in order to test our classical retrieval models.

- [`cords`](src/cords): Implements Java classes with the following functionalities:
  1. [`CollectionReader.java`](src/cords/CollectionReader.java): Reading and parsing the TREC-COVID collection files.
  2. [`Poolindexing.java`](src/cords/PoolIndexing.java): Indexing the collection into an Apache Lucene index.
  3. [`PageRank.java`](src/cords/PageRank.java): Computing the references graph between articles of the collection.
  4. [`QueryComputation.java`](src/cords/QueryComputation.java): Computing the queries of each topic of the TREC-COVID Challenge.
  5. [`QueryEvaluation.java`](src/cords/QueryEvaluation.java): Evaluating our retrieval models in the TREC-COVID Challenge.

- [`formats`](src/formats): Defines file structures of the collection in order to parse its content. 
  - [`Article.java`](src/formats/Article.java) is used for the PMC and PDF JSON files in [`document_parses/`](2020-07-16/document_parses).
  - [`Metadata.java`](src/formats/Metadata.java) is used for each row of [`metadata.csv`](2020-07-16/metadata.csv) CSV file.
  - [`RelevanceJudgements.java`](src/formats/RelevanceJudgements.java) is used for each line of 
  [`relevance-judgements.txt`](2020-07-16/relevance-judgements.txt) TXT file.
  - [`Topics.java`](src/formats/Topics.java) is used for each item of [`topics-set.xml`](2020-07-16/topics-set.xml) XML file.

- [`lucene`](src/lucene): Is an abstraction of the original Apache Lucene classes [IndexWriter](https://lucene.apache.org/core/7_4_0/core/org/apache/lucene/index/IndexWriter.html), 
[IndexReader](https://lucene.apache.org/core/8_0_0/core/org/apache/lucene/index/IndexReader.html) and 
[IndexSearcher](https://lucene.apache.org/core/8_0_0/core/org/apache/lucene/search/IndexSearcher.html) that handles exception throws.
- [`models`](src/models): Implementation of the classical retrieval models (see the [next section](#implemented-models)).
- [`schemas`](src/schemas): Our own classes to store variables and easily implement parsing, indexing and querying 
processes.
- [`util`](src/util): Auxiliary static functions that are used for all classes in order to afford code. 

## Implemented models

In this section we briefly explain the implemented retrieval models. Note that these models are not intended to provide 
the best results in the TREC-COVID Challenge, but to show how classical retrieval models work for academical purposes.

- [Boolean Weighted Model](https://nlp.stanford.edu/IR-book/html/htmledition/boolean-retrieval-1.html): 
It uses `title`, `abstract` and `body` fields with weights $20$, $10$ and $5$ respectively. The documents' scores are computed 
with the [Language Retrieval Model](https://nlp.stanford.edu/IR-book/html/htmledition/language-models-for-information-retrieval-1.html) 
using Jelinek-Mercer smoothing.
- [Vector Model](https://nlp.stanford.edu/IR-book/html/htmledition/vector-space-classification-1.html): 
It uses the `embedding` field of each document and the 
topics embeddings (stored in [`topic-embeddings.json`](2020-07-16/topics-embeddings.json)) to compute a 
[KnnVectorQuery](https://lucene.apache.org/core/9_2_0/core/org/apache/lucene/search/KnnVectorQuery.html) and then apply 
the [Rocchio algorithm](https://nlp.stanford.edu/IR-book/html/htmledition/the-rocchio-algorithm-for-relevance-feedback-1.html) 
to obtain new query embeddings. The parameters used for Rocchio can be manually configured in the `VectorModel` class. 
By default, we use $\alpha=0.5$, $\beta=0.4$ and $\gamma=0.1$, and the number of reranking iterations is $5$.
- [Probability Model](http://nlp.stanford.edu/IR-book/html/htmledition/probabilistic-information-retrieval-1.html): It 
computes a Boolean Weighted Query using the [Probabilistic Retrieval Model](https://nlp.stanford.edu/IR-book/html/htmledition/probabilistic-information-retrieval-1.html) 
and reranks the initial ranking by [expanding the query with new terms](https://nlp.stanford.edu/IR-book/html/htmledition/query-expansion-1.html).
- [PageRank Model](https://nlp.stanford.edu/IR-book/html/htmledition/pagerank-1.html): It uses the Boolean Weighted Model to compute 
initial results and then reranks the initial ranking using the Page Rank of each document. Note that Page Rank is obtained 
at indexing time.

### Considerations about the Page Rank implementation

In order to obtain the graph of references between documents, we manually implement a searching process where, for each 
document:

1. We obtained information about its bibliography entries and how many times in the body text each entry was cited.
2. For each bibliography entry (in the code documentation this is also called `reference`) we create a BooleanQuery and 
search the title and authors of the entry in the index. We create a match between each bibliography entry and the top `m`
documents obtained.
3. Matches are saved as vectors in the index.

Thus, once the PageRank process has finished, in the index we have the following information per document $d_i$ (for $i=1,...,n$ where $n$ is the number of documents in the collection):

- A vector $\vec{t}^{(i, c)} = (t^{(i,c)}_1,..., t^{(i,c)}_n)$ with reference information where $t^{(i,c)}_j$ is the number of times $d_i$ references to $d_j$ considering the cite counts.
- A vector $\vec{t}^{(i,nc)}$ that is obtained via normalizing $\vec{t}^{(i, c)}$ following Page Rank algorithm:

$$ \text{norm}({\vec{t}_c}) = \begin{cases}
  (1/n | j=1,...,n) & \text{if }t_j=0, \ \forall j \in [1,n] \\[1em]
  \vec{t}_c\cdot\frac{1-\alpha}{\text{sum}(\vec{t}_c)} + \frac{\alpha}{n} & \text{otherwise}
\end{cases}
$$
- A vector $\vec{t}^{(i,nb)} = \text{norm}(\vec{t}^{(i,b)})$ where $\vec{t}^{(i,b)} = \mathbb{I}( \vec{t}^{(i,c)} \geq 1)$ is the binarization of $\vec{t}^{(i,c)}$.
- Invert vectors: 

$$ \begin{cases}
  \vec{o}^{(i,nb)} = (o^{(i,nb)}_1,...,o^{(i,nb)}_n), \quad \text{where } o_j^{(i,nb)} = t^{(j, nb)}_i \\[1em]
  \vec{o}^{(i,nc)} = (o^{(i,nc)}_1,...,o^{(i,nc)}_n), \quad \text{where } o_j^{(i,nc)} = t^{(j, nc)}_i
  \end{cases}
  $$

*Note*: $o$ vectors are the column vectors obtained by row-stacking $t_i$ for $i=1,...,n$ in normalized vectors in a matrix.

With the inverse-references normalized vectors we can compute the PageRank algorithm until convergence.


## Results

## Execution times



