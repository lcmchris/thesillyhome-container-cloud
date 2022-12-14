# Welcome to the thesillyhome-container-cloud

# Introduction

The idea behind a second repo is to differentiate the local and cloud version. Why would you want a cloud version you might ask?
With an existing local setup, unless you deploy on a sufficiently large machine there is no way to train big models. Because of the limitation the local version actually has limits setup so that your little rasberry pi don't blow up due to insufficient memory or runtime.
Additionally, at some point a generalized model will help drastically with performance.

# Archeticture

There are significant differences on architecture. This will be setup to communicate with the API created and hosted by The Silly Home.
1. Storing data
2. Parsing and learning
3. Delivering models