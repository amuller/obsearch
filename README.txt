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




command line: (GPL)
http://getpot.sourceforge.net/

http://jchassis.sourceforge.net/