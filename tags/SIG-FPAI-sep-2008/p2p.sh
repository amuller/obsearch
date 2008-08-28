
# this is a script used to run the p2p example.
# you should customize it to your network.
# you should define one or more seeders (rendezvouz and relay)
# and update the file 

# we have already created an index,
# rsync everything

rsync -az --exclude=pom.xml obsearch 192.168.1.86:~/gsoc/
