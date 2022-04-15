
# GCED-Information Retrieval Assignment- Iteration 2

*Documentation of the implemented Java code for the second iteration of the GCED-Information Retrieval assignment (15/04/2022).*

This Markdown contains all the details about the implemented Java classes and methods in order tackle Iteration-2 objectives:

1. Parse the [test set of 50 topics](https://ir.nist.gov/trec-covid/data/topics-rnd5.xml) of the TREC-COVID Collection.  
2. Search over the index created in Iteration-1 using the `query` field of each topic.
3. Generate the results using the [submission TREC-COVID Challenge format](https://ir.nist.gov/trec-covid/round1.html).
4. Parse the [relevance judgements](https://trec.nist.gov/data/qrels_eng/) using the defined format.
5. Evaluate the results using the relevance judgements by computing the MAP@k metric.

In the `es.udc.fi.irdatos.c2122.cords` package of Maven Project we find multiple Java files, each of them with its 
own functionality:

- The Java classes corresponding to the Iteration-1 are: `Article.java`, `Metadata.java`, `CollectionReader.java`, 
`IndexMetadataPool.java` and `ReadIndexMetadata.java`. Some files have changed since the last commit of March. In order 
to make indexing process faster and make the code more legible, a new class for parallel indexing was implemented and 
the same methods were "distributed" in different classes following their own meaning.
- The Java classes corresponding to this iteration (Iteration-2) are: `Topics.java`, `RelevanceJudgements.java`, 
`QueryTopics.java` and `ReadQueryTopics.java`.


*Note*. To execute and test the provided code we recommend executing the classes `ReadIndexMetadata.java` for the 
Iteration-1 and `ReadQueryTopics.java` for the Iteration-2. In addition, the working directory is needed to have the 
following structure, so the execution can find the correct files to read and parse.

````text
--- root/                    # this is your working directory!
    --- 2020-07-16/          # here all files are stored
        --- document_parses/
            --- pdf_json/
            --- pmc_json/
        --- metadata.csv
        --- relevance_judgements.txt
        --- topics_set.xml
````

From this point we will see the methods implemented in each class and how they are used to get each one of the 
aforementioned goals of the iteration.

## Changes in the Iteration-1

As it was mentioned above, the classes of the Iteration-1 have changed in order to provide a faster execution 
for the indexing process. Most of the methods implemented are exactly the same (so we recommend reading `README-Iteration1`) 
just their "organization" has changed.

In the `IndexMetadataPool.java` it is implemented the parallel indexing process using multiple workers that are 
managed by the `Executor`. Once the `metadata.csv` file has been parsed, this class takes this content and starts 
the indexing process *while reading the JSON files*. So, each worker of the pool is not only in charge of 
indexing a set of articles (via a set of rows of the CSV), but also of reading and parsing the PMC and PDF files 
provided in each row of the `metadata` object.

In the `CollectionReader` we find the same methods of the first version of this iteration code that allowed us to read 
and parse the CSV and JSON files. They have been relocated since it is easier to read the parsing methods in the same class 
and the indexing implementation in a different class.

Finally, the `ReadIndexMetadata.java` class, where all the methods (parsing + indexing) were previously located, is 
now used only for testing and evaluating the indexing process. Note that, from this class, it is possible to change 
the index configuration (`Analyzer`, `Similarity`, etc.) and folder names to store the execution results. In order 
to make tests or create different types of inverted indexes, this code-structure has helped us to make the process 
faster and cleaner than using the old code-structure.


## Classes and methods of the Iteration-2

Following the same steps for reading and parsing JSON and CSV files, the XML and TXT new files (topics set and 
relevance judgments) were parsed using the [Jackson data format module](https://github.com/FasterXML/jackson-dataformat-xml). 
The structure followed and parsed can be consulted in `Topics.java` and `RelevanceJudgements.java` files.

The `ReadQueryTopics.java` class implements both the "parsing operation" and "evaluation process". The methods 
implemented are:

- `readTopicSet()` and `readRelevanceJudgements()`, in charge of reading and parsing the topics XML file and 
the relevance judgments using `Topics.java` and `RelevanceJudgements.java`.
- `averagePrecision()` and `meanAveragePrecision()`, in charge of evaluating the list of documents obtained from the 
searching process of each topic.
- `generateResults()`, in charge of generating the `txt` file with the querying results with the [format](https://ir.nist.gov/trec-covid/round1.html) 
defined in the TREC-COVID Challenge.

From the `ReadQueryTopics.java` class, methods of `QueryTopics.java` class are called. This class (`ReadQueryTopics.java`), 
as its name indicates, implements different types of queries for each topic (but only using the `query` field). It is 
intended to extend this class with more methods implementing different types of queries and test their performance via 
the MAP@k metric. So far only two different types of queries have been used with similar MAP@k value:

- `simpleQuery()`: Searches each individual word of the topic query in the title, abstract and body fields with 
some weighting (it was used 0.3 for title, 0.4 for abstract and 0.3 for body since it had a better performance).
- `phraseQuery()`: A more sophisticated query that uses phrase searching with some slop using *bigrams* (sequences 
of 2 consecutive words in the topic query), *trigrams* (sequences of 3 consecutive words in the topic query) and 
the whole topic query sentence in the title, abstract and body fields with a given boosting. 

Both queries had a similar performance using the MAP@k metric. However, in the future is intended to think about 
more effective queries to continuously improve the searching results.