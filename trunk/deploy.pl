#!/usr/bin/perl -w

# This script performs all the steps necessary for a distribution.
# It will create the src and bin assemblies and 
# will upload all of them to google code. It will also
# update the website in berlios.de
# parameters: <user_name> <google code pwd>

# generate the website
#system("mvn site:site") or die "Could not generate site";
# generate the assemblies
#system("mvn assembly:assembly") or die "Could not generate assemblies"


$user = $ARGV[0];
$password = $ARGV[1];

@files = `ls ./target/obsearch*`;

foreach $x (@files){
		chomp($x);
		uploadFileToGoogleCode($x);
}

#finally add the site update code!
		




sub uploadFileToGoogleCode {
		my($file) = @_;
		my $comment;
		if($file =~ /bin/){
				$comment = "binary";
		}elsif($file =~/jar/){
				$comment = "jar-no-dependencies"
		}
		else{
				$comment = "source";
		}
    
		my $md5 = sum($file,"md5");
		my $sha1 = sum($file,"sha1");
		
    print "$file $md5 $sha1\n";
my $cmd = "python googlecode_upload.py  --summary=$comment -p obsearch -l $md5,$sha1  --config-dir=./.svn -u $user -w $password $file";
		print "executing $cmd\n";
		system($cmd);
    
}


sub sum {
		my($file, $p) = @_;
		my @m = split(/\s/, `$p $file`);
		my $res = $m[0];
		return "$p:$res";
}
