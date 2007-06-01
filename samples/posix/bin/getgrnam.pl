#!/usr/bin/perl

if ($#ARGV == -1) {
    $groupname = `/usr/bin/whoami`;
    chomp($groupname);
} else {
    $groupname = $ARGV[0];
}

print "Calling getgrnam(\"$groupname\")...\n\n";

($name, $password, $gid, $members) = getgrnam($groupname);

print "Name     : $name\n";
print "Password : $password\n";
print "GID      : $gid\n";
print "Members  : $members\n";
