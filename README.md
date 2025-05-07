# Scalable-Hebrew-Word-Prediction-Using-Trigram-Probabilities-on-AWS-EMR
A scalable Hebrew word prediction system using trigram probabilities, implemented with Hadoop MapReduce and deployed on AWS EMR. Processes the Google Books Ngram corpus to build a knowledge-base for next-word prediction.

The goal is to compute the conditional probability of each trigram (w1, w2, w3) in the corpus as:
P(w3 | w1, w2) using a smoothing technique based on frequency backoff.
