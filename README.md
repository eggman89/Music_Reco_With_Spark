# Music Recommendation Using Spark

This project attempts to answer the following 3 questions :

1) Given the listetning habit of a user, how can we reccomend him the best songs suiable to his profile?
We use a hybrid of collabarative filtering, popularity based model and content based filtering to achieve this.
(RecoEngine.scala)
2)Given a set of songs and it's attributes (i.e tempo, energy, dancablity and loudness) , can we predict the tag for new songs(e.g. CLassic Rock, Pop etcc..)?
(tagGenerator.scala)

The dataset has beenn aquired from MillionSongDataset, LastFM and echonest APIs.

More details would be added soon.

->Implemented hybrid of Collaborative filtering and Content-based recommendation using Mllib(logistic regression) .
->Implemented automatic tag generation(using logistic regression, Random Forest, Naive Bayes and Decision tree) for new songs based on historical data 
->Provides the prediction accuracy and time taken by all the four algorithms.

The cleaned and parsed data can be downloaded from here:
https://drive.google.com/a/ncsu.edu/folderview?id=0B5_HzOkbztHuMkptSzJidzl1c1k&usp=sharing

The architecture is as follows:
http://imgur.com/a/0adNo

Run the following two files:
tagGenerator.scala
RecoEngine.scala
