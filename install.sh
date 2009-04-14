# this script is only for OBSearch developers
# this script loads the latest versions of some dependencies not 
# available in maven repositories

# get the latest version of je
wget http://obsearch.googlecode.com/files/je-3.2.44.jar

mvn install:install-file -Dfile=./je-3.2.44.jar -DgroupId=berkeleydb -DartifactId=je -Dversion=3.2.44 -Dpackaging=jar -DgeneratePom=true

#cleanup

rm je-3.2.44.jar

# get fmpp
wget http://obsearch.googlecode.com/files/fmpp_0.9.12.tar.gz
tar -xzf fmpp_0.9.12.tar.gz


# install fmpp (freemarker's ant frontend)
mvn install:install-file -Dfile=./fmpp_0.9.12/lib/fmpp.jar -DgroupId=freemarker -DartifactId=fmpp -Dversion=0.9 -Dpackaging=jar -DgeneratePom=true

#cleanup
rm -fdr fmpp_0.9.12
rm fmpp_0.9.12.tar.gz



# install jxta

mkdir temp

cd temp 
wget http://download.java.net/jxta/jxta-jxse/2.5/jxse-lib-2.5.tar.gz

tar -xzf jxse-lib-2.5.tar.gz

mvn install:install-file -Dfile=jxta.jar -DgroupId=jxta -DartifactId=jxta -Dversion=2.5 -Dpackaging=jar -DgeneratePom=true

cd ..
#cleanup
rm -fdr temp 


wget http://obsearch.googlecode.com/files/trove-2.0.1.jar
mvn install:install-file -Dfile=trove-2.0.1.jar -DgroupId=trove -DartifactId=trove -Dversion=2.0.1 -Dpackaging=jar -DgeneratePom=true




rm trove-2.0.1.jar


mvn install:install-file -Dfile=db.jar -DgroupId=bdb-c -DartifactId=bdb-c -Dversion=4.7 -Dpackaging=jar -DgeneratePom=true


mkdir hilbert
cd hilbert

wget http://uzaygezen.googlecode.com/files/uzaygezen-0.1.zip
unzip uzaygezen-0.1.zip


mvn install:install-file -Dfile=core-0.1.jar -DgroupId=google -DartifactId=uzaygezen -Dversion=0.1 -Dpackaging=jar -DgeneratePom=true

wget http://google-collections.googlecode.com/files/google-collect-snapshot-20080820.zip
unzip google-collect-snapshot-20080820.zip

cd google-collect-snapshot-20080820

mvn install:install-file -Dfile=google-collect-snapshot-20080820.jar -DgroupId=google -DartifactId=google-collect -Dversion=20080820 -Dpackaging=jar -DgeneratePom=true

cd ..


wget http://www.meisei-u.ac.jp/mirror/apache/dist/commons/lang/binaries/commons-lang-2.4-bin.tar.gz
tar -xzf commons-lang-2.4-bin.tar.gz

 cd commons-lang-2.4/

mvn install:install-file -Dfile=commons-lang-2.4.jar -DgroupId=apache -DartifactId=commons -Dversion=2.4 -Dpackaging=jar -DgeneratePom=true

cd ..
cd ..
rm -fdr hilbert


# install weka


wget http://www.obsearch.net/weka-3-5-8.zip

unzip weka-3-5-8.zip


cd weka-3-5-8/

mvn install:install-file -Dfile=weka.jar -DgroupId=weka -DartifactId=weka -Dversion=3.5.8 -Dpackaging=jar -DgeneratePom=true

cd ..

rm -fdr weka-3-5-8/



#infovis

wget http://www.obsearch.net/infovis.jar

mvn install:install-file -Dfile=infovis.jar -DgroupId=inria -DartifactId=infovis -Dversion=0.9 -Dpackaging=jar -DgeneratePom=true

rm infovis.jar


wget http://www.csie.ntu.edu.tw/~cjlin/cgi-bin/libsvm.cgi?+http://www.csie.ntu.edu.tw/~cjlin/libsvm-2.88.tar.gz
tar -xzf libsvm-2.88.tar.gz
cd libsvm-2.88 
cd java

mvn install:install-file -Dfile=libsvm.jar -DgroupId=libsvm -DartifactId=libsvm -Dversion=2.88 -Dpackaging=jar -DgeneratePom=true

cd ..
cd ..

rm -fdr libsvm-2.88
rm libsvm-2.88.tar.gz



# install opt4j
mkdir opt4j
cd opt4j

wget obsearch.net/opt4j-1.5.1.zip

unzip opt4j-1.5.1.zip

cd opt4j-1.5.1/

mvn install:install-file -Dfile=opt4j-1.5.1.jar -DgroupId=opt4j -DartifactId=opt4j -Dversion=1.51 -Dpackaging=jar -DgeneratePom=true

cd ..

rm -fdr opt4j-1.5.1/



wget obsearch.net/fastutil-5.1.5.jar

mvn install:install-file -Dfile=fastutil-5.1.5.jar -DgroupId=mg4j -DartifactId=fastutil -Dversion=5.1.5 -Dpackaging=jar -DgeneratePom=true

rm fastutil-5.1.5.jar


wget obsearch.net/dsiutils-1.0.7.jar

mvn install:install-file -Dfile=dsiutils-1.0.7.jar -DgroupId=mg4j -DartifactId=dsiutils -Dversion=1.0.7 -Dpackaging=jar -DgeneratePom=true

rm dsiutils-1.0.7.jar

wget obsearch.net/mg4j-2.1.2.jar

mvn install:install-file -Dfile=mg4j-2.1.2.jar -DgroupId=mg4j -DartifactId=mg4j -Dversion=2.1.2 -Dpackaging=jar -DgeneratePom=true

rm mg4j-2.1.2.jar



wget obsearch.net/dependencies/tokyocabinet.jar
mvn install:install-file -Dfile=tokyocabinet.jar -DgroupId=tc -DartifactId=tc -Dversion=1.17 -Dpackaging=jar -DgeneratePom=true

rm tokyocabinet.jar
