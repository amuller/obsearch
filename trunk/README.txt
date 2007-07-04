Synopsis:

This project is to similarity search what 'bit-torrent' is to downloads. A p+tree is a high dimension index structure. This project consists on implementing a p+tree in a distributed environment. All the clients will share the workload when performing queries.  

Details:

Similarity searches are required in many areas. Examples are sequences matching, music matching and program matching. In the case where an exact match is required, it is possible to access the data in constant time. In the case of similarity search, one has to use special indexing techniques to reduce the amount of comparisons that have to be performed. There has been much research on the subject of similarity matching and several approaches that work well in practice have been developed. All these approaches are CPU intensive. This of course limits the amount of clients a server can hold.

Among these indexing techniques, the pyramid technique is of special interest. In this approach, all the data is divided into an i number of pyramids (the user specifies i). A query can be answered by looking only at the pyramids that intersect it. It is very natural then to separate each pyramid into a client and apply a distributed approach for answering queries.

This project could benefit different communities that require similarity matching services just as audio, source code, video, biology, weather forecasts, etc. 

By using these ideas, CPU-intensive information retrieval can be performed with just a few servers. Monetary cost is reduced considerably. Also, the approach is very general. The only thing that the user has to provide is a distance function that satisfies the triangular inequality. Also they could provide an 'almost metric' and with some tweaking the function can be forced to satisfy the triangular inequality.


The first time it is run please execute 
./install.sh

Compiling
mvn compile

Using with Eclipse

Do the following:
mvn -Declipse.workspace=/home/<usr>/workspace eclipse:add-maven-repo
mvn eclipse:eclipse 
Maybe you should do a "mvn eclipse:clean" before commiting files


how to make a branch:1

svn copy https://obsearch.googlecode.com/svn/trunk/ \
             https://obsearch.googlecode.com/svn/branches/obsearch-floating-fix \
             -m "I will change the way values are mapped, to reduce precision loss... this may be the problem I have"  --username <you>

30 min



Grid library:
http://dsd.lbl.gov/firefish/


merge:

Take the stuff from the first location into the second, and use
the working directory to evaluate the new changes
You can do a revert if things don't work ok.
 
svn merge https://obsearch.googlecode.com/svn/branches/obsearch-floating-fix https://obsearch.googlecode.com/svn/trunk/


---------------------------------------------------
Notes
---------------------------------------------------
Definitions:

Golden peer: Holds the current database id. Without the golden peer we cannot insert
             data.
x: is the pyramid # for the element we want
e: element to be inserted
box: A partition of the space (the space is partitioned into n)

- Insertion:
  * Ask a number of peers if they have our element.
    (we don't want to get an id for something that is already in the DB)
  * Get an id from the golden peer.
	* Broadcast a message to all the peers who hold x and ask them to insert e.
	* If any peer already has e, send a message to all the peers who hold x and ask
    them to delete e.
- Merging
	* From time to time and when our peer starts, we will do a "merge"
	* We need to find ways of performing this merge. This merge also deletes elements
    that were deleted
- Deletion
	* Propagate a deletion command to all the corresponding peers.

Every peer has a box distribution 
[[box1, count]... [boxn, count]]

Every count indicates the number of peers that are serving the respective box
When a peer gets in the network, he chosses where boxes to serve by using this

Matching can be constrained to a relatively local (and close) set of peers.

As soon as we log in, we start connecting to all the peers. We have
to connect to all the boxes before we start matching.

One option is to just access pipes.


JXTA links:
Things to check when JXTA is not working
http://wiki.java.net/bin/view/Jxta/NetworkBasics

Private network demo:
http://www.petrovic.org/blog/2006/11/15/a-turnkey-private-jxta-net-demo/







Screenshot:

4 cpus (Intel quad core 64 bit, 4GB ram)

Assert mode on:

Sequential:

25 min??? (maybe 200 was with 

P+Tree:
7.4 min () (od=6)

30min (od=2)

5 min (od=8)

5.5min (od=10)

Assert mode off:



Perf numbers:

Sequential
24 min

Dummy pivot selection
Extended pyramid
12 min

P+tree
3.7 min

P+Tree (2 cpus)
7.42 min... WHY!


command line: (GPL)
http://getpot.sourceforge.net/

http://jchassis.sourceforge.net/



Example <<P+Tree>>:
-----------------------

OD=8 
Tentacle
448 sec  7 min (100 mb cache)
454 sec 7 min (300mb cache)

OD=6
533 minutes: 8(300mb cache)


125 minutes for 88 pivots!
Very fast actually.