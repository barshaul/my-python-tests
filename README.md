# Communicator-app
Small Python application for data communication with two processes running on the same machine. 
Randomly generated vectors will be sent over a socket from the first process to the second process.
These “data” vectors would be accumulated in the second process in a matrix. 
Results will be saved to a csv file.

To use the application please pass timeout in seconds as an argument.
For example, to run the data communication app for 1 minute, execute:

python ./communicator_app.py 60