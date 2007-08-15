#!/usr/bin/perl -w

# if the parameter "clean" is used, then 
# all the java processes are destroyed and the 
# program exists

# the database folder
$dbRoot = "~/temp/";
$dbFolder = "$dbRoot/PurpleTentacle7";
# the location of the spore file
$sporeFolder = "$dbFolder/std/";
$spore = "$sporeFolder/PPTreeShort";

# where we created the database
$databaseCreated = "192.168.1.86";
# the servers that you will use
@servers = ("192.168.1.86", "192.168.1.85", "192.168.1.81");
# the search by default will be performed in the machine
# this script is running

if($ARGV[0] eq "clean"){
		foreach my $x (@servers){
				killJava($x);
		}
		exit;
}

if($ARGV[0] eq "empty"){
		foreach my $x (@servers){
				if(! ($x eq $databaseCreated)){
						empty($x);
				}
		}
		exit;
}

if($ARGV[0] eq "copy"){
		foreach my $x (@servers){
				if(! ($x eq $databaseCreated)){
						copy($x);
				}
		}
		exit;
}


# rsync everybody
foreach my $x (@servers){
		sync($x);
}

# copy spore
foreach my $x (@servers){
		if(! ($x eq $databaseCreated)){
		       copySpore($x);
    }
}

foreach $x (@servers){
		if($x eq $databaseCreated){
				p2pserver($x, 16);
		}else
		{ 
				p2ptentacle($x, 16);
		}
		sleep(10);
}

# now we just have to run the search algorithm

shell("ant -buildfile example.xml p2psearch");


# now we can call our searcher
# the searcher will wait and connect until all the data has been transfered
#shell(mkCommand("local", 16, "p2psearch"));


sub killJava{
		my($ip) = @_;
		`ssh $ip \"killall java\"`;
}

sub empty {
		my($ip) = @_;
		ssh($ip, "rm -fdr $dbFolder");
}

# copy the spore first by transfering it to our machine and
# then by copying it to the destination
sub copySpore{
		my($ip) = @_;
		print "Copying spore for $ip\n";
		ssh($ip, "mkdir -p $sporeFolder");	
		shell("mkdir -p $sporeFolder");
		shell("rsync $databaseCreated:$spore $spore");
		shell("rsync $spore $ip:$spore");
}


# copy the db folder first by transfering it to our machine and
# then by copying it to the destination
sub copy{
		my($ip) = @_;
		print "Copying spore for $ip\n";
		shell("rsync -az $databaseCreated:$dbFolder $dbRoot");
		shell("rsync -az $dbFolder $ip:$dbRoot");
}

# call remote peers
sub p2ptentacle{
		my($ip,  $threads) = @_;
		p2pAux($ip, $threads,"p2ptentacle");
}

sub p2pserver{
		my($ip,  $threads) = @_;
		ssh($ip,"rm -fdr $dbFolder");
		ssh($ip,"cp -r $dbFolder" .  "BKP $dbFolder");
		p2pAux($ip, $threads,"p2pserver");
		
}

sub p2psearch{
		my($ip,  $threads) = @_;
		p2pAux($ip, $threads, "p2psearch");		
}

sub p2pAux{
		my($ip,  $threads, $cmd) = @_;
		`ssh $ip \"killall java\"`;
		sshE($ip, "/bin/bash -i  -c 'cd ~/gsoc/obsearch/; " . mkCommand($ip, $threads, $cmd));
}

# makes the ant command
sub mkCommand{
		my($ip, $threads, $cmd) = @_;
		return "ant -buildfile example.xml $cmd -Dname=${ip}' -Dthreads=$threads";
}

# exec a command via ssh
# uses the & command so that control returns immediatly to the caller
sub sshE {
		my($ip, $cmd) = @_;
		shell("ssh $ip \"$cmd\"&");
}

sub ssh{
		my($ip, $cmd) = @_;
		shell("ssh $ip \"$cmd\"");
}

# sync a computer
sub sync{
		my($ip) = @_;
		#shell("rsync --exclude=pom.xml --exclude=example.xml -az . $ip:~/gsoc/obsearch/");
		shell("rsync  -az . $ip:~/gsoc/obsearch/");
}


# execute a shell command
sub shell {
    my($cmd) = shift;
    my $status = system($cmd);
    die "Command failed: $cmd\n" unless $status == 0;
}
