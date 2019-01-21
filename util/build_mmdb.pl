use strict;
use warnings;
use feature qw( say );
use Text::CSV_XS qw( csv );
use Net::Works::Network;
use MaxMind::DB::Writer::Tree;
use Data::Dumper;

my $filename = 'users.mmdb';
# Read whole file in memory
my $aoh = csv (in => "GeoLite2-City-Locations-en.csv", headers => "auto");

# geoname_id
# locale_code
# continent_code
# continent_name
# country_iso_code
# country_name
# subdivision_1_iso_code
# subdivision_1_name
# subdivision_2_iso_code
# subdivision_2_name
# city_name
# metro_code
# time_zone
# is_in_european_union

my %lookup;

foreach my $item (@$aoh) {
	$lookup{$item->{'geoname_id'}} = $item;
}

my %types = (
	country_code       => 'utf8_string',
	city               => 'utf8_string',
	is_anonymous_proxy => 'boolean',
	latitude           => 'float',
	longitude          => 'float'
);

my $tree = MaxMind::DB::Writer::Tree->new(
    database_type => 'my_custom_data',

    # "description" is a hashref where the keys are language names and the
    # values are descriptions of the database in that language.
    description =>
        { en => 'My database of IP data', fr => "Mon Data d'IP", },

    # "ip_version" can be either 4 or 6
    ip_version => 4,

    # add a callback to validate data going in to the database
    map_key_type_callback => sub { $types{ $_[0] } },

    # let the writer handle merges of IP ranges. if we don't set this then the
    # default behaviour is for the last network to clobber any overlapping
    # ranges.
    merge_record_collisions => 1,

    # "record_size" is the record size in bits.  Either 24, 28 or 32.
    record_size => 24,
);

# network
# geoname_id
# registered_country_geoname_id
# represented_country_geoname_id
# is_anonymous_proxy
# is_satellite_provider
# postal_code
# latitude
# longitude
# accuracy_radius

my $csv = Text::CSV_XS->new ({ binary => 0, auto_diag => 1 });
open my $fh, "<", "GeoLite2-City-Blocks-IPv4.csv";
$csv->header($fh);
while (my $row = $csv->getline_hr($fh)) {
	my $country_code = $lookup{$row->{'geoname_id'}}->{'country_iso_code'};
	my $city = $lookup{$row->{'geoname_id'}}->{'city_name'};

	my $metadata = {};

	if(defined($country_code) and $country_code ne '') {
		$metadata->{'country_code'} = $country_code;
	}

	if(defined($row->{'is_anonymous_proxy'})) {
		$metadata->{'is_anonymous_proxy'} = $row->{'is_anonymous_proxy'};
	}

	if(defined($row->{'longitude'})) {
		$metadata->{'longitude'} = $row->{'longitude'};
	}

	if(defined($row->{'latitude'})) {
		$metadata->{'latitude'} = $row->{'latitude'};
	}

	if(defined($city) and $city ne '') {
		$metadata->{'city'} = $city;
	}

	$tree->insert_network( $row->{'network'}, $metadata );
}

close $fh;

open $fh, '>:raw', $filename;
$tree->write_tree( $fh );
close $fh;

say "$filename has now been created";
