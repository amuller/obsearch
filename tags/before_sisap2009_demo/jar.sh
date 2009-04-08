


# creates OBSearch's jar and installs it into the local
# dir. Receives the target version of the jar that we will install.

OBVERSION=$1

mvn jar:jar
cd target
mvn install:install-file -Dfile=obsearch-$OBVERSION.jar -DgroupId=gsoc -DartifactId=obsearch -Dversion=$OBVERSION -Dpackaging=jar -DpomFile=pom.xml

cd ..