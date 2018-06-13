# Data-Driven Encoding Selector
This project explores an automated method to select efficient lightweight encoding schemes for column-based databases, such as run length and bitmap encoding. Comparing to popular compression techniques like gzip and snappy, lightweight encoding has the following benefits:
* Speed: Lightweight encodings involve only simple operations and usually has negligible impact on data access time. 
* Local Computation: Most Lightweight encoding schemes only access adjacent local tuples during encoding/decoding process, without the need to go through entire dataset.
* In-Site Query Execution: Encoding schemes enable in-place query execution that allows queries to be directly applied on encoded data and further reduce the time needed for query processing, instead of requiring a large block of data to be uncompressed first.

However, there is no simple rule on how to choose the best encoding scheme for a given attribute. Existing systems either rely on database administrators' personal experience or use simple rules to make selection. In practice, neither of these two methods achieve optimal performance. Our method attacks the problem in a systematic data-driven way. Our effort includes:
* Dataset Collection
* Pattern Mining
* Data-Driven Encoding Prediction
 
## Dataset Collection
Our work is based on analysis and evaluation of real-world datasets. We have created an automated framework to collect datasets, extract columns from them, organize, and persist the records for further analysis. The framework could accept various input formats including csv, txt, JSon and MS Excel files. It also supports recognition of column data type for unattended data collection. For further analysis purpose, the framework provides API enabling customized features to be extracted from the columns.

Using this framework, we have collected over 7000 columns from approximately 1200 datasets with a total size of 500G data. These datasets are all from real-world data sources and cover a rich collection of data types (integer, date, address, etc.), with diverse data distributions. We use Apache Parquet's built-in encoders to encode these data columns with different encoding schemes, looking for the one performing best for each column. We also developed some customized encoders to compare their performance.

[Insert some charts we generated for the dataset]

Here's a list of data sources we used in the experiments.
* Government Data Portals
  * [Open Government Data](https://www.data.gov/open-gov/)
  * [NYC](https://opendata.cityofnewyork.us/)
  * [Chicago](https://data.cityofchicago.org/)
  * [Baltimore](https://data.baltimorecity.gov/)
  * [Dallas](https://mydata.dallasisd.org/)
  * [Washington D.C.](https://dc.gov/page/open-data)
  * [LA](https://data.lacity.org/)
  * [Maryland](https://data.maryland.gov/)
  * [Lousiville](https://data.louisvilleky.gov/)
  * [Oakland](https://data.oaklandnet.com/)
* Server Logs
  * Argonne Server Logs [Requesting permission to publish]
* GIS data
  * [ArcGIS Open Data](http://hub.arcgis.com/pages/open-data)
  * [Esri Open Data](http://www.esri.com/software/open/open-data)
* Social Networks
  * [Bikeshare](https://www.bikeshare.com/)
  * [Yelp](https://www.yelp.com/dataset/challenge)
* Machine Learning Datasets
  * [UCI Repo](https://archive.ics.uci.edu/ml/)


## Pattern Mining
Data type is crucial to encoding selection. A proper data type determination can greatly reduce space requirement for encoded data. For example, storing a date field in string format requires at least 8 bytes, while storing it in integer format takes no more than 4 bytes. If we further observe the effective data range, this can be further reduced to 23 bits. However, most real-world datasets are semi-structured, in which only part of the data contains valid common structures. Pattern Mining targets at automatically identify and extract these structures, allowing more efficient encoding to be applied on them. In this project, we have developed two methods for Pattern Mining, **Common Sequence** and **Frequent Similar Words**.

### Common Sequence
First, we notice that many patterns can be described by a sequences of tokens. E.g. a telephone number can be described by the sequence "(NUM) NUM-NUM". To look for such patterns, Common Sequence method first splits records into sequences of tokens, then look for longest non-overlapping common sub-sequences of tokens from them. 

When comparing tokens, we first make sure they are of the same type. Noticing the fact that number in general has a better compression ratio than text, we treat number tokens as equal regardless of the actual content, but force text token to be equal only when the content is the same. We then use Dynamic Programming to implement an  O(n^2l) algorithm to look for common tokens. It is then straight-forward to convert the token sequences into regular expressions that can be used for data extraction and compression.

### Frequent Similar Words
Some patterns involve understanding of natural languages, which are obvious to human eyes, but are much harder for program to learn. For example, a typical U.S. address has the following pattern: \<Apt No.> [E|S|W|N] \<Road Name> [Rd.|Ave.|St.] \<City> \<State>, \<ZIP>. However, to extract this pattern from a list of addresses, program must have the knowledge that "Rd.", "St." and "Ave." are similar things and thus can be used as separator. To achieve this, Frequent Similar Words use NLP techniques to recognize and group these words.

The algorithm first look for words appearing multiple times in different records ("frequent words"), compute their frequencies, and group the words by their similarity while maintaining their relative sequences. E.g., record 1 contains "apt" and "st.", record 2 contains "p.o. box" and "rd.". As "st." and "rd." are more similar, the grouping result will be ("apt","p.o. box") and ("rd.", "st."). These groups are then used as separators to split text into finer sequences, allowing us to further looking for common patterns. 
 
We use GloVe(Global Vectors for Word Representation) from Stanford NLP group to determine the similarity of words. The dataset maps words into vectors and maintains their semantic similarity. That is, semantically similar words will have higher cosine similarity. The dataset contains 2.2 million vocabularies, which covers most daily words.

## Data Driven Encoding Prediction

In this project, we target at building a encoding selector that is able to "predict" the best encoding scheme by looking at only a limited section of the entire dataset, e.g., the first 1% of the records. To do this, we plan to use the collected data columns and the "ground truth" best encoding scheme to train a neural network for this purpose. 

Choosing core features that truly represents the relationship between data and label is crucial to the success of neural network training task. The features we choose including statistical information such as mean and variance of data length, entropy. We also use NLP technique to learn from column names. Finally, we also try to use LSTM RNN network to learn patterns from the sampled column. 


