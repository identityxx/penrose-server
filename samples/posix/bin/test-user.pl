#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = `/usr/bin/whoami`;
    chomp($name);
}

print "Calling getpwnam(\"$name\")...\n\n";

($name, $password, $uid, $gid, $quota, $comment, $gecos,
 $dir, $shell, $expire) = getpwnam($name);

print "Name     : $name\n";
print "Password : $password\n";
print "UID      : $uid\n";
print "GID      : $gid\n";
print "Quota    : $quota\n";
print "Comment  : $comment\n";
print "Gecos    : $gecos\n";
print "Home     : $dir\n";
print "Shell    : $shell\n";
print "Expire   : $expire\n";
