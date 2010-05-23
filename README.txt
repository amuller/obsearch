   ____  ____ _____                      __  
  / __ \/ __ ) ___/___  ____ ___________/ /_ 
 / / / / __  \__ \/ _ \/ __ `/ ___/ ___/ __ \
/ /_/ / /_/ /__/ /  __/ /_/ / /  / /__/ / / /
\____/_____/____/\___/\__,_/_/   \___/_/ /_/ 


Version: ${version}

*********
Synopsis:
*********

OBSearch is a similarity search engine.

* Details
~~~~~~~~~~

 Similarity search is required in many areas. For example, music matching and binary program matching require a similarity search engine. Nowadays, it is common to hear news of projects like "photosynth" that heavily rely on similarity search. OBSearch is a similarity search engine that can help you to create an interesting and new application!

	This project started as part of Google Summer of Code 2007. The mentoring organization was Portland State University.

**************************
Information for Developers
**************************

 Requirements:
 -------------

* You need a JDK (We have tested OBSearch with Java 1.6.0_01).
* You need to have a recent version Maven and ANT installed and working
  (We have tested OBSearch with Maven 2.0.6 and 2.0.7 and ANT 1.7.0).


The first time you checkout OBSearch do a:

Add the following to your .m2/settings.xml

<repositories>
...
    <repository>
      <id>obsearch</id>
      <url>http://obsearch.net/repository</url>
    </repository>
...
  </repositories>


For Tokyo Cabinet and Berkeley DB you may need to rebuild the jars and
install them manually in your local respository. 
See the file install.sh for an example on how to do this.


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
The test will run for a while, if everything is fine, then you can release:
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

You may have to add the variable M2_REPO to the Libraries tab (right click project/properties/java build path). This variable should point to Maven's repository: ~/.m2/repository/

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


 Berlios notes
 ----------
 help on berlios can be found here:
 http://developer.berlios.de/docman/display_doc.php?docid=43&group_id=2
