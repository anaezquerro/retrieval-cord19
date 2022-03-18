# GCED-Information Retrieval Assignment- Iteration 1

*Documentation of the first iteration Java code of GCED-Information Retrieval assignment (18/03/2022).*

This Markdown contains all the details about Java classes and methods implemented to tackle Iteration-1 objectives:

1. Parse CSV and JSON files content relative to TREC-COVID articles.
2. Index relevance *items* of this content using Apache Lucene utilities learned in class.

In the `es.udc.fi.irdatos.c2122.cords` package of Maven Project we find three classes:

- `ReadIndexMetadata.java`: Parses `metadata.csv` file content and uses its columns (only few are considered) to 
construct Fields and add them to Documents. 
- `ArticleMetadata.java`: Defines CSV scheme of `metadata.csv` (columns) to get rows content as a List.
- `Article.java`: Defines JSON scheme of each PMC/PDF article file.

Note in the first lines of `ReadIndexMetadata` the necessary folder structure to import the document collection and `metadata.csv` file.

````text
--- root
    --- cord-19_2020-07-16
        --- 2020-07-16
            --- document_parses
                --- pdf_json
                --- pmc_json
            --- metadata.csv
````


## `ReadIndexMetadata` class

Implements the complete process of parsing `metadata.csv` and articles files (in JSON format), and indexing the most relevant fields of the obtained data (in this iteration the relevance of an item is directly related to its usability in Iteration-2). 
The result of its execution is the creation of a folder in which the inverted indexes are stored.  


This class contains five methods; each of them in charge of performing the aforementioned steps. 
The following sections describe each method and its purpose.


### `readArticle` method

Reads JSON file and deserializes its content. As PMC and PDF files have the same structure, both types can be read with this method.

- Arguments: 
  - `articlePath (Path)`: Java path representation of the JSON file to parse.
- Returns:
  - `articleContent (String[])`: As we see in the code, the returned variable `articleContent` 
  is meant to have three `string` elements: the text of the article (i.e. *body_text* field of JSON field), the concatenated 
  titles of references (i.e. *bib_entries.title*) and the concatenated notes of figures and tables in the article (i.e. *ref_entries*).

### `readArticles` method

Using `readArticle` method, reads multiple JSON files with a list of paths provided and concatenates its per-field (of interest) content.

- Arguments:
  - `articlesPath`: List of Java path objects of JSON files.
- Returns:
  - `articlesContent`: String array of three elements (note it has the same length of the `readArticle()` returned array) 
, where each element corresponds with the concatenation of a same field (of interest) for all JSON file.

The purpose of this function is to deal with the problem of having multiple PDF files in one row of the metadata CSV. 
Via a descriptive analysis of `metadata.csv` implemented with Python (this code is not provided), few realizations were reached:

1. There is no article (i.e. row) in metadata with more than one PMC file.
2. Some articles (rows) have more than one PDF file.

The official [GitHub repository of TREC-COVID Challenge](https://github.com/allenai/cord19) informates about this issue 
and proposes multiple solutions when having more than one PDF file: parsing the first file (in this iteration it was 
checked that, in these cases, the attributes of first PDF file were the same as the corresponding `metadata.csv` row), 
parsing all files or, in case PMC file exists, only parsing PMC file (we discuss this issue in the `indexMetadata` section).

However, it is advised that having multiple PDF path files does not mean each PDF file is one article, it is just a different
"representation" of the same article. That is the reason we considered desirable to condense all PDFs information 
(of the same article) in the same three fields (note that these fields will be passed as a Field of the Lucene Document).


### `readMetadata` method

Using `ArticleMetadata` class as the CSV scheme, reads `metadata.csv` and returns each row as a list of `ArticleMetadata` elements.

### `indexMetadata` method

With the parsed content of `readMetadata()` `readArticle()` and `readArticles()` methods, creates an inverted 
index considering each article (i.e. metadata row information) as a Document with the following Fields:

- `docID`: Directly obtained from the `metadata.cord_uid` column. Stored type (but neither indexed nor tokenized).
- `title`: Obtained from `metadata.title` column. Indexed, tokenized and stored.
- `abstract`: Obtained from `metadata.abstract` column. Indexed, tokenized and stored.
- `body`: Obtained via the concatenation of sections titles and paragraphs of the article (using `readArticle()` or 
`readArticles()` method).
- `references`: Obtained from the concatenation of references title of the article. Indexed, tokenized and stored.
- `figures`: Obtained from the concatenation of text notes in figures of the article. Indexed, tokenized and stored.

To create the Document of each article, it was necessary to parse the JSON files to get the information relative to the 
body text, references and figures. Following the recommendations of [GitHub repository of TREC-COVID Challenge](https://github.com/allenai/cord19) 
about parsing PMC and/or PDF files, in this iteration it was considered to:

1. Prefer PMC file content over PDF files, as they are two different representations of the same article (i.e. the information
they contain is almost the same) and the PMC file is "cleaner" to parse.
2. In case of not having PMC file, parsing PDF file(s).
3. In case of not having PMC or PDF file paths, saving the Fields `body`, `references` and `figures` as an empty String.


Note these considerations might change in future iterations.


## `ArticleMetadata` class

Defines the `metadata.csv` structure to parse its columns content.

In the process of inverted index construction, the selection of columns in `metadata.csv` is critical, and this 
importance increases with their number and referenced files quantity. For this assigment the chosen fields to index
are the following:

- `cord_uid`: assigns an identifier for each paper contained in the provided data. Type: `str`.
- sha: hash of all the documents (if any) associated with the current. Type: `List[str]`.
- source_x: sources from where the dataset received the current paper. Type: `List[str]`.
- `title`: paper tittle. Type: `str`.
- doi: paper DOI, Digital Object Identifier. Type: `str`.
- pmcid: paper's identifier assigned on PubMed Central. Type: `str`.
- pubmed_id: paper's identifier assigned on PubMed. Type: `str`.
- license: license associated with the current paper, Type: `str`.
- `abstract`: paper's abstract. Type: `str`.
- journal: name from the journal on which the current paper was published. Type:`str`.
- mag_id: currently deprecated, used to be the identifier for the Microsoft Academic Graph. Type: `int`.
- who_convidence_id: identifier assigned on the WHO, World Health Organization. Type: `str`.
- arxiv_id: identifier assigned for the arXiv, paper free distribution service. Type: `str`.
- `pdf_json_files`: contains paths to the parses of the papers PDFs into JSON format. Type: `List[str]`.
- `pmc_json_files`: contains paths to the parses of the papers from PMC, PubMed Central, for XML into JSON format. Type: `List[str]`.
- url: contains all URLs associated with the current paper. Type: `List[str]`.
- s2_id: contains the Semantic Scholar ID for the paper. Type: `str`.

Note highlighted fields are the parsed ones.


## `Article` class

Defines the JSON article files schema (fields per level) to extract the most relevant information to index. 

Once the metadata has been parsed, the next step is to process the JSON files of the articles which contains any available. 
In addition to the selected fields from the metadata file, the following fields have been selected from the JSON files:

- `body_text`: `List[str]` valued field composed of the different parts of the text (`text[str]` field), as well as the 
section title (`title[str]`) to which they belong.
- `bib_entries`: Parsed as a `Map<String, Reference>` object whose keys are obtained from the references identifiers (`str`)
and map to a `Reference` object constructed from each corresponding reference title.
- `ref_entries`: Similar to `bib_entries` but in this case the references are to items of the paper itself 
(figures and tables).

