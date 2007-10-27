   ____  ____ _____                      __  
  / __ \/ __ ) ___/___  ____ ___________/ /_ 
 / / / / __  \__ \/ _ \/ __ `/ ___/ ___/ __ \
/ /_/ / /_/ /__/ /  __/ /_/ / /  / /__/ / / /
\____/_____/____/\___/\__,_/_/   \___/_/ /_/ 


Version: ${version}

*********
Synopsis:
*********

This project is to similarity search what 'bit-torrent' is to
downloads. All the clients share the workload when performing
queries.

********
Details:
********

Similarity search is required in many areas. Examples are DNA sequences, music and program matching. When an exact match is required, it is possible to access the data very efficiently. In the case of similarity search, one has to use special indexing techniques to reduce the amount of comparisons that have to be performed. There has been much research on the subject and several approaches that work well in practice have been developed. All these approaches are CPU intensive. This of course limits considerably the amount of clients a server can hold.

Individuals or small companies who want to provide similarity search services cannot afford the infrastructure required to support such indexes. OBSearch can be used to distribute the workload among the users and reduce the investment to a few "seeder" servers.

OBSearch achieves this by dividing the search space into n "boxes". Each client that joins the network will support m boxes (where m is much smaller than n). Additionally, OBsearch can determine for any query which boxes have to be searched. This allows OBSearch to efficiently perform similarity searches. The library is modular, so you can also use OBSearch in a single computer. It is very fast! http://planet-soc.com/node/1527

This project could benefit different communities that require similarity matching services on audio, source code, video, biology data, etc.

By using these ideas, CPU-intensive information retrieval can be performed with just a few servers. Infrastructure cost is reduced considerably. Also, the approach is very general. The only thing that the user has to provide is a distance function that satisfies the triangular inequality(1).

This project is being developed as part of Google Summer of Code 2007. The mentoring organization is Portland State University.

Technicalities:

Among similarity search indexing techniques, the pyramid technique is of special interest. In this approach, all the data is divided into an i number of pyramids (the user specifies i). A query can be answered by looking only at the pyramids that intersect it. It is very natural then to separate each pyramid into a client and apply a distributed approach for answering queries. The pyramid technique can only match vectors. In order to be able to match any kind of object, a dimension reduction technique called SMAP (Simple Map) is being employed. To further improve performance, we are employing a P+Tree (partitions the space so that the pyramid technique becomes more efficient) combined with K-means++.

(1) Also the users can provide an 'almost metric' and with some tweaking the function can be forced to satisfy the triangular inequality.

**************************
Information for Developers
**************************

 Requirements:
 -------------

* You need a JDK (We have tested OBSearch with Java 1.6.0_01).
* You need to have a recent version Maven and ANT installed and working
  (We have tested OBSearch with Maven 2.0.6 and 2.0.7 and ANT 1.7.0).

The first time you checkout OBSearch do a:
./install.sh 

This will download and install all the necessary dependencies.

Whenever you do svn update and install.sh is changed please
run install again.

***********************************
Information for OBSearch Developers
***********************************

 Deployment:
 -----------
Before deploying, change the line in pom.xml from
<my.test.data.db>slices-small</my.test.data.db>
to:
<my.test.data.db>slices</my.test.data.db>

And run: mvn test
The test will run for 40 hours, if everything is fine, then you can release:
but first restore the line <my.test.data.db>slices-small</my.test.data.db>
in pom.xml

perl deploy.pl

This script will generate the binary files, upload the website to
berlios.de and will also generate an announce.txt file ready to 
be sent to the mailing lists. The label creation is a manual process
 and it must be done after this script has been completed:
 
svn copy https://obsearch.googlecode.com/svn/trunk/ \
             https://obsearch.googlecode.com/svn/tags/0.7-GSOC \
             -m "initial release"  --username <you>

 Compiling:
 ----------

mvn compile


*********************
Information for Users
*********************

Learning how to use OBSearch:

The easiest way is to check the tutorial in the website.
You can base your code on:
org.ajmm.obsearch.example.OBExampleTrees.java
For single computer users, and on:
org.ajmm.obsearch.example.OBSearchExample.java
For p2p users.
We highly recommend you to start with the single computer tutorial.

Stability:

OBSearch has two modes of operation. Single computer and p2p operation.

Single Computer:
We have a comprehensive set of tests that verify that every object
inserted into the database is searched, deleted and stored properly.
The full tests take around 40 hours and are always executed before publishing
a new version of the program. 
You can see the summary of the results in the project's website under
"Project Reports".

P2P Module:
We cannot generate tests that are as predictable as in the single
computer case.  Our test harness creates several peers in
different computers, and the peers synchronize correctly and searches
also work. We would like people to try out this module and give us
feedback.


*************
Known issues:
*************

PPTreeShort does not work for range == 0. 
Workaround: 
Use the ExtendedPyramidIndexShort class that does not suffer
from this issue.
or 
If O.distance(O) == 0 <=> O.equals(O) then you can
use the function exists(). Exists is implemented in a
more efficient way and it is faster than calling searchOB(...)
with range 0.

Ranges > 0 for PPTreeShort are working properly.


********************
Running the Examples
********************


First you have to make sure that "mvn test" works properly.
If it doesn't please post an error on the mailing lists. It is normal
to see many files being downloaded by maven the first times
you run the examples or compile the program.

 Single Computer:

1) mvn compile
# creates a new database
2) ant -buildfile example.xml create
# searches the datatabase
3) ant -buildfile example.xml search

Play with the create and search targets and try different
parameters!

 P2P Module:

Requires that all the machines have ssh with rsync installed.
You should have ssh-agent configured or you will have to type
your password many times. You also need ant and maven installed.

Preparation:


You have to create the database in one machine. This machine's
ip address will be "CIP". You should also run the p2p.pl script
from a computer that is not CIP.

0) mvn test 
1) Edit the file src/main/resources/seeds.txt
   Put replace the ip that is written there with CPIP
2) (In CPIP) ant -buildfile example.xml p2pcreate
3) mvn compile
4) Edit p2p.pl
4.1) Find the variable: $databaseCreated
	 Its assignment must look like $databaseCreated = CPIP;
4.2) Find the variable: @servers
	 Put all the ips of the machines involved including CPIP;
5) Copy all the files from the current directory into
		 the folder ~/gsoc/obsearch of all the machines that will be
		 used in the test. Run ./install.sh on each of the machines.

Running the example:
WARNING: Because some java processes stay alive even after
killing the calling processing (when using ssh) this Perl script
will kill all the Java processes in the host machine before and
after executing.

Running:

perl p2p.pl

You have to wait until the peers find each other, and synchronize the
data. When the data is synchronized a search will be performed.

Running again p2p.pl will load the peers, but because the sync has
been performed, then no data transfer will be done and the search
will be executed immediately.

If you want to see the sync process again: 

perl p2p.pl empty

This will delete the indexes in all the peers except the 
peer in CPIP.

Note:
If you want to change the base directory in which the
databases will be stored you have to modify the following:
Property dbfolder in example.xml
Variable $dbFolder in p2p.pl
You have to make sure that both have the same value. Sorry for the
inconvenience.

*******************************
Additional notes for developers
*******************************

 Using maven with Eclipse:
 -------------------------

# do this line only once
mvn -Declipse.workspace=/home/<usr>/workspace eclipse:add-maven-repo

# do this line every time you change the project's dependencies or
any major thing that could affect eclipse:

mvn eclipse:eclipse 

 Testing:
 --------

mvn test

Note:
If you want to do the 29 hour test, then replace this line
<my.test.data.db>slices-small</my.test.data.db> in pom.xml:
by:
<my.test.data.db>slices</my.test.data.db>


 Build website:
 -----------
 Builds the latest version of OBSearch's website.

mvn site


 How to make a branch:
 ---------------------

svn copy https://obsearch.googlecode.com/svn/trunk/ \
             https://obsearch.googlecode.com/svn/branches/mynewbranch \
             -m "my new branch"  --username <you>

 How to make a tag (label):
 --------------------------

svn copy https://obsearch.googlecode.com/svn/trunk/ \
             https://obsearch.googlecode.com/svn/tags/0.7-GSOC \
             -m "initial release"  --username <you>

--- 

 Packaging:
 ----------
 
 mvn assembly:assembly



Planed Features:


* Random pyramid selection and priorities for space tree nodes. (this is actually slow, left here for documentation purposes)