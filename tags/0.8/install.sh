
# this script loads the latest versions of some dependencies not 
# available in maven repositories

# get the latest version of je
wget http://obsearch.googlecode.com/files/je-3.2.23.tar.gz
tar -xzf je-3.2.23.tar.gz

mvn install:install-file -Dfile=./je-3.2.23/lib/je-3.2.23.jar -DgroupId=berkeleydb -DartifactId=je -Dversion=3.2.23 -Dpackaging=jar -DgeneratePom=true

#cleanup
rm -fdr je-3.2.23
rm je-3.2.23.tar.gz

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
wget http://download.java.net/jxta/jxta-jxse/2.5_rc2/jxse-lib-2.5_rc2.tar.gz

tar -xzf jxse-lib-2.5_rc2.tar.gz

mvn install:install-file -Dfile=jxta.jar -DgroupId=jxta -DartifactId=jxta -Dversion=2.5rc2 -Dpackaging=jar -DgeneratePom=true

cd ..
#cleanup
rm -fdr temp 


wget http://obsearch.googlecode.com/files/trove-2.0.1.jar
mvn install:install-file -Dfile=trove-2.0.1.jar -DgroupId=trove -DartifactId=trove -Dversion=2.0.1 -Dpackaging=jar -DgeneratePom=true

rm trove-2.0.1.jar