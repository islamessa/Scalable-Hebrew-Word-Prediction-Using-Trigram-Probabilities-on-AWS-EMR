This project implements a scalable knowledge-base generator for Hebrew word prediction, 
based on the Google Books Ngram 3-gram dataset using Amazon Elastic MapReduce (EMR) and Hadoop MapReduce.

Given the corpus of trigrams (sequences of 3 Hebrew words), the system calculates the conditional probability of a third word (w3) given the previous two words (w1, w2).
This predictive model is commonly used in Natural Language Processing (NLP) tasks such as auto-completion, suggestion systems, and text generation.
