
# this script loads the latest versions of some dependencies not 
# available in maven repositories
# get the latest version of je
wget http://obsearch.googlecode.com/files/je-3.2.23.tar.gz
tar -xzf je-3.2.23.tar.gz

mvn install:install-file -Dfile=./je-3.2.23/lib/je-3.2.23.jar -DgroupId=berkeleydb -DartifactId=je -Dversion=3.2.23 -Dpackaging=jar -DgeneratePom=true


# mvn install:install-file -Dfile=./velocity-1.5.jar -DgroupId=velocity -DartifactId=velocity -Dversion=1.5 -Dpackaging=jar -DgeneratePom=true

#cleanup
rm -fdr je-3.2.23
rm je-3.2.23.jar
