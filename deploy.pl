#!/usr/bin/perl -w

# This script performs all the steps necessary for a distribution.
# It will create the src and bin assemblies and 
# will upload all of them to google code. It will also
# update the website in berlios.de
# parameters: <user_name> <google code pwd>

$user = $ARGV[0];
$password = $ARGV[1];

if (not defined $user ){
		die "You did not give me a user name!";
}

if (not defined $password){
		die "You did not give me a password!";
}

# generate the website
shell("mvn site:site");
# generate the assemblies
shell("mvn assembly:assembly");




# upload the distribution files
@files = `ls ./target/obsearch*`;
$fileMsg = "";
$version = "";

foreach $x (@files){
		chomp($x);
		uploadFileToGoogleCode($x);
}



# now generate the notice
open OUT, ">announce.txt"	or die "Could not create announce file";

print OUT <<EOF;

OBSearch $version released.

OBSearch $version has been released.

Release Highlights:

You can download it from:
http://code.google.com/p/obsearch/downloads/list

$fileMsg

Please send any bug reports to:
http://code.google.com/p/obsearch/issues/list

Homepage:
http://code.google.com/p/obsearch/

Website:
http://obsearch.berlios.de/


Arnoldo Muller

EOF

close OUT;


{   local( $| )= 1;
    print "Will deploy the site, press [Enter] to continue...";
    my $resp= <STDIN>;
}

shell("mvn site:deploy");

print "\n\nPlease run the following command when you are sure everything is ok\n";
print "svn copy https://obsearch.googlecode.com/svn/trunk/ https://obsearch.googlecode.com/svn/tags/$version -m 'LABEL: $version'  --username <you>\n";

sub uploadFileToGoogleCode {
		my($file) = @_;
		my $comment;
		if($file =~ /bin/){
				$comment = "binary";
		}
		elsif($file =~/jar/){
				$comment = "jar-dist";
						($v) = ($file =~ /obsearch-(.*)[.]jar/);
				$version = $v;
				#print "Version found! $version";
		}
		else{
				$comment = "source";
		}
    
		my $md5 = sum($file,"md5");
		my $sha1 = sum($file,"sha1");
		$sfile = $file;
		#file without the prefix dir
		$sfile =~ s/[.]\/target\///g;
    print "$sfile $md5 $sha1\n";
		$fileMsg =  "$fileMsg\n$md5 $sha1 $sfile";
		my $cmd = "python googlecode_upload.py  --summary=$comment -p obsearch -l $md5,$sha1  --config-dir=./.svn -u $user -w $password $file";
		shell($cmd);
    
}


sub sum {
		my($file, $p) = @_;
		my @m = split(/\s/, `$p $file`);
		my $res = $m[0];
		return "$p:$res";
}


sub shell {
    my($cmd) = shift;
    my $status = system($cmd);
    die "Command failed: $cmd\n" unless $status == 0;
}
