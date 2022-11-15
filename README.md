Data Mining Project for assignment Mining of Massive Datasets

Authors: Manuel Montoya - Omar Alejandro Henao

Folders:

container/ has the code ran inside the EC2 container in AWS to group and move the tweets from json to parquet.
data/ has the test data for the initial tests done on the draft.
notebooks/ has the notebooks used for the sample dataset where the tests where devised and checked.
pipeline/ has the files used for the extract, transform and load steps of the processing.

Files:
requirements.txt has the required modules to run the project.

To run this project, you should have an .env file with the aws credentials set in. Be aware that access keys that start with ASIA require a session token, due to their temporary nature.
