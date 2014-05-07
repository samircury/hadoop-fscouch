#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Time::ParseDate;
use CouchDB::Client::DB;
use CouchDB::Client;
use CouchDB::Client::Doc;

my $input = $ARGV[0] || "fsimage-red.del";

sub main {
        
    # Get the XML itself
    my $input_image = shift;
    #if (not -e $input_image) {
    #    die "Please provide an existing input as the first argument";
    #}
    # Connect to the database
    my $c = CouchDB::Client->new(uri => 'http://user:pw@user.cloudant.com');
    $c->testConnection or die "The server cannot be reached";
    my $db = $c->newDB('db');

    # Iterate through the (possibly huge) input file
    open FILE, "<$input_image";
    my $documents_submitted_per_cycle = 5000;
    my $counter = 0;
    my @documents = ();
    while (<FILE>) {

        my @fields = split('\*\*',$_);
        my $document = process_document(\@fields, $db);
        
        push(@documents,$document) if $document;
        if (scalar( @documents ) == $documents_submitted_per_cycle) {
            print "submitting \n";
            # Submit documents
            $db->bulkStore(\@documents);
            print(scalar(@documents)."\n");
            # empty array
            @documents = ();
            print(scalar(@documents)."\n");
        }
        
    }
    close FILE;	
    # In case we still have the last N < per_cycle documents in the
    # array, submit them now
    if (scalar( @documents ) > 0 ) {
	print "submitting \n";
	$db->bulkStore(\@documents);
	print(scalar(@documents)."\n");
	@documents = ();
	print(scalar(@documents)."\n");
    }
}

sub process_document{
    my $input_aref = shift;
    my $db = shift;
    my @fields = @{$input_aref};

    my %inode;
    my $lfn = $fields[0];
    $inode{'replication'} = $fields[1];
    $inode{'mtime'} = parsedate($fields[2]);
    $inode{'atime'} = parsedate($fields[3]);
    $inode{'size'} = $fields[4] * $fields[5] if $fields[5] && $fields[4] ; # nblocks * blocksize
    $inode{'owner'} = $fields[11];
    $inode{'group'} = $fields[12];

    # Eliminating a lot of overhead by dropping checksums
    return undef if  $lfn =~ /.*cksum.*/;

    if ($inode{'size'} && $inode{'size'} > 200) {
        my $document = CouchDB::Client::Doc->new( 'id' => $lfn ,'data' => \%inode, 'db' => $db);   
        return $document;
    }
}


main($input);
