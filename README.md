# hadoop-domain-name-parse
A simple domain name parsing program that uses distributed file systems to digest large files of domain names

Takes advantage of Apache Hadoop to parse out all domain names from a file such as a ZONE File that can be acquired
from a DNS server (although 99% impossible due to security issues)

Inspired by some work i've done in big data analysis.

Setup
======

Setup from Hadoop assumes that you followed the official Hadoop getting started tutorial found [Here](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

Assuming there is a directory for hadoop and the **start-dfs.sh** script has been run, and the environment variable __HADOOP_CLASSPATH__ has been set. You can run the test script to try a test run.


Run Example files
------

You can run the example script **example-run.sh** with the following:

```bash
./example-run.sh -d <dictionary-file-location.txt> -h <hadoop-directory>
```

Where -d is a custom dictionary file. The default will be the dictionary.txt file that is provided with this repostiory.
Where -h is the directory to Hadoop according to the SingleCluster setup example provided in the Setup section.

The test run will run different tests from the test directories and store the results in the same directories. There is an expected result output that is sorted in order to compare md5 sums between both expected output and actual output files.


Credits
------

https://github.com/opendns/public-domain-lists - free sample list of domain names used in the testing
https://github.com/dwyl/english-words - free dictionary used for this project. Strongly recommend a more up-to-date that includes trendy and slang words, but this will suffice for the time being.
