# Music Recommendation Using Spark

This project attempts to answer the following 3 questions :

1) Given the listetning habit of a user, how can we reccomend him the best songs suiable to his profile?
We use  a hybrid of collabarative filtering and content based filtering to achieve this.

2)Given a set of songs and it's attributes (i.e tempo, energy, dancablity and loudness) , can we predict the tag for new songs(e.g. CLassic Rock, Pop etcc..)?

The dataset has beenn aquired from MillionSongDataset, LastFM and echonest APIs.

More details would be added soon.

->Implemented hybrid of Collaborative filtering and Content-based recommendation using Mllib(logistic regression) .
->Implemented automatic tag generation(using logistic regression, Random Forest, Naive Bayes and Decision tree) for new songs based on historical data 
->Provides the prediction accuracy and time taken by all the four algorithms.
