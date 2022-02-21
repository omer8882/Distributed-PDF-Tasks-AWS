# AWS Task Distribution

This project uses Amazon Web Services to distribute workload of mass PDF convertions to other formats.
The application is composed of a local application and instances running on the Amazon cloud. The local application will get a text file as an input containing a list of URLs of PDF files alongside an operation to perform on them. Then, instances (called workers) will be launched in AWS. Each worker will download PDF files, perform the requested operation, and display the result of the operation on a webpage
This project was written as part of the Distributed Systems course in Ben Gurion University semester 1/2022 by Omer Dahary and Niv Dan.

## Table of contents
* [Description](#Description)
    * [Local Application](#1.-Local-Application)
    * [Manager](#2.-Manager)
    * [Workers](#3.-Workers)
* [Running the Application](#Running-the-Application)
    * [Setup](#Setup)
    * [Excecuting Local Application](#Excecuting-Local-Application)
* [Impelementation notes](#Notes-on-Implementation)
* [Tests](#Tests)

## Description

This repository contains the code for the 3 elements that compose the entire system: Local Application, Manager and Worker(s).
The elements communicate with each other using SQS and S3 sevices provided by AWS.
All messages sent between the elements is done by useing SQS queues. And all files are tranfered through uploading and/or downloading via S3.

### 1. Local Application

This is the application run by the user. 
The input is a text file containing any number of lines. Each line will contain an operation followed by a tab ("\t") and a URL of a pdf file.
The operation can be one of the following:
* ToImage - convert the first page of the PDF file to a "png" image.
* ToHTML - convert the first page of the PDF file to an HTML file.
* ToText - convert the first page of the PDF file to a text file.

The output is an HTML file containing a line for each input line. The format of each line is as
follows: 
```
<operation>: <input file> <output file>
```
* Operation is one of the possible operations.
* Input file is a link to the input PDF file.
* Output file is a link to the image/text/HTML output file.
If an exception occurs while performing an operation on a PDF file, or the PDF file is not available,
then output line for this file will be: 
```
<operation>: <input> file <a short description of the exception>.
```

The Local App checks if there is an EC2 instance of a Manager running, if not it creates one. Then uploads the input file to S3 along with a message request.
Then it waits for a comeplete message from the Manager along with the required result.

### 2. Manager
The manager process resides as a EC2 instance. At all times there is only one manager instance that handles the local application requests. It has the ability to handle multiple Local Application requests simultaneously. The Manager checks a special SQS queue for messages from local
applications. Once it receives a message it:
* If the message is that of a new task it:
	* Downloads the input file from S3.
	* Creates an SQS message for each URL in the input file together with the operation that should be performed on it.
	
* If the message is a termination message, then the manager:
	* Does not accept any more input files from local applications.
	* Waits for all the workers to finish their job, then terminates them and itself, in a safe way.

While checking for new messages, the manager computes the number of worker instances it needs to activate, using the number of SQS operation messages. The manager creates a worker for every n messages (n being the input of the local application input who creates the Manager). 
Note that while the manager creates a node for every n messages, it does not delegate messages to specific nodes. All of the worker nodes take their messages from the same SQS queue.

### 3. Workers
A worker process resides on an EC2 instance. Its life cycle is as follows (repeatedly):
* Get a message from Workers SQS queue.
* Download the PDF file indicated in the message.
* Perform the operation requested on the file.
* Upload the resulted output file to S3.
* Send a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
* remove the processed message from the SQS queue.

If an exception occurs, then the worker recovers from it, sends a message to the manager of the input message that caused the exception together with a short description of the exception, and continues working on the next message.
If a worker stops working unexpectedly before finishing its work on a message, then some
other worker should be able to handle that message.

## Running the Application
### Setup
First, you need to compile and package the 3 element of the project:
* To compile and package the LocalApplication jar file use the command: 
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
* To compile and package the Manager and Worker, use the same command while making sure beforehand that their respective Classes will be running in executing the jars (usually done with in the POM file).
Because S3 bucket names are unique across the entine service, if you desire to run the project yourself, you should update the communal bucket names in the code for your specific usage.
After packaging the Manager and Worker jars correctly, you will upload them to your main bucket.

### Excecuting Local Application
* Make sure your updated AWS credentials are in the "credentials" file in ".aws" folder in your OS's user folder.
* Locate your input file in the same folder as the LocalApplication.jar
* Run the following command line in the same folder the LocalApplication.jar file is located:
```
java -jar LocalApplication.jar [input file] [output file] [num of files per worker] [terminate]
```
* where:
    * LocalApplication.jar is the name of the jar file containing the code of LocalApplication.
    * inputFileName is the name of the existing input file.
    * outputFileName is the name of the desired output file.
    * n is the workers’ files ratio (how many PDF files per worker).
    * terminate indicates that the application should terminate the manager at the end.

Note that you can run multiple local applications at once, but once there is an active Manager on termination, new Local Apps will not be dealt with.

## Notes on Implementation

### General notes
* The application is secure, because AWS credentials are only stored on the client's computer. They are not passed online as is. Only using the proper IAM role.
* AMI "ami-00e95a9222311e8ed" is used for EC2 instances (Linux T2_MICRO with Java 8 installed) both for Manager and Workers.
* We set the visibility time of a single Worker request message on SQS to be 20 minutes and of a whole client task to be 75 minutes for the posibility that if a worker or the manager stops working and needs to restart, the SQS message request would be picked up again. If the request was completed, then the sqs message would be removed within that timeframe.
* We used a thread per client for the manager, to allow clients requests to run simultaniously.

### Dependencies
The project uses the following dependencies:
* software.amazon.awssdk
    * EC2
    * S3
    * SQS
* org.apache.pdfbox
    * pdfbox
    *pdfbox-tools
* org.apache.maven.plugins
    * maven-assembly-plugin
    * maven-jar-plugin
    * maven-compiler-plugin

## Tests

As a running example, a couple of test file cases are attached to the project in the folder "Test Files", as well as their outputs.

### Notes
* The tests used `n = 10` as the number of files per workers.
* Within the manager we set the limit of maximum workers active simultaniously to be 8 (because the AWS service shuts instances unprompted if there are more than that, when using a student account).
* It took approximately 30 minutes for the "input-sample-1.txt" file to complete.

## Authors

Omer Dahary and Niv Dan