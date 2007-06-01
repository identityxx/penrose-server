#!/usr/bin/perl

if ($#ARGV == -1) {
    $username = `/usr/bin/whoami`;
    chomp($username);
} else {
    $username = $ARGV[0];
}

print "Calling getpwnam(\"$username\")...\n\n";

($name, $password, $uid, $gid, $quota, $comment, $gcos,
 $dir, $shell, $expire) = getpwnam($username);

print "Name     : $name\n";
print "Password : $password\n";
print "UID      : $uid\n";
print "GID      : $gid\n";
print "Quota    : $quota\n";
print "Comment  : $comment\n";
print "Gecos    : $gcos\n";
print "Home     : $dir\n";
print "Shell    : $shell\n";
print "Expire   : $expire\n";
