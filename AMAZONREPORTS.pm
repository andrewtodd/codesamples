package SYNDICATION::AMAZONREPORTS;

### OVERVIEW
# 	There are many reports that are available in Seller central that merchants would like to access from Zoovy. Additionally, there are many reports 
#	that are NOT available on seller central that are available via the reports API. 
#
# AMAZONREPORTS...
#		1. checks which reports the merchants want.
#		2. requests that Amazon create each report * (using the request_report subroutine)
#		3. gets a list of available reports (have already been created) from Amazon (using the 'run'** subroutine 
#		3. retrieves each report and saves it into PRIVATE_FILES (using the get_report subroutine)
#		5. acknowledges the report (using ack_reports subroutine)
#
#		*	Settlement and Sales Tax reports can't be requested. They are automatically scheduled by Amazon.
#		**	although every subroutine can be ran independently the 'run' subroutine will go through steps 2, 3 and 4.
#		 


use Data::Dumper;
use strict;
use Digest::HMAC_SHA1;
use MIME::Base64;
use Digest::MD5;
use XML::SAX::Simple;

use lib "/httpd/modules";
require ZOOVY;
require DIME::Parser;
require ZTOOLKIT;
require AMAZON3;
require PRODUCT;

## usage
#
#		request_report: perl -e 'use lib "/httpd/modules"; use SYNDICATION::AMAZONREPORTS; my ($so) = SYNDICATION->new("andrewt","AMZ",PRT=>"#0"); SYNDICATION::AMAZONREPORTS::request_report($so,'_GET_MERCHANT_LISTINGS_DEFECT_DATA_');'	
#		run: perl -e 'use lib "/httpd/modules"; use SYNDICATION::AMAZONREPORTS; my ($so) = SYNDICATION->new("andrewt","AMZ",PRT=>"#0"); SYNDICATION::AMAZONREPORTS::run($so);'	
#		test_request_report: perl -e 'use lib "/httpd/modules"; use SYNDICATION::AMAZONREPORTS; my ($so) = SYNDICATION->new("andrewt","AMZ",PRT=>"#0"); SYNDICATION::AMAZONREPORTS::test_request_report($so);'	
#		


### get_permissions
#		get_permissions returns the list of reports types selected by the merchant in  Marketplace -> Amazon -> Reports 
#		
sub get_permissions {
	my ($so, %options) = @_;

	my $USERNAME = $so->username();

	my $lm = $options{'*LM'};
	if (not defined $lm)	{
		$lm = LISTING::MSGS->new($USERNAME,logfile=>"~/amazon-reports-%YYYYMM%.log",'stderr'=>0);
		}

	my @REPORTS = ();
	## NOTE: .amazon_reportpermissions and .fba_reportpermissions are stored as bitwise values
	if (((int($so->get('.amazon_reportpermissions'))>0)) || ((int($so->get('.fba_reportpermissions'))>0))){
		## At least one report has been selected in Marketplace -> Amazon -> Reports 
		foreach my $report(keys %SYNDICATION::AMAZONREPORTS::AMAZON_REPORTS) {
			if (int(($so->get('.amazon_reportpermissions'))& $SYNDICATION::AMAZONREPORTS::AMAZON_REPORTS{$report})>0) {
			   push @REPORTS, $report;
				}
			}
		foreach my $report(keys %SYNDICATION::AMAZONREPORTS::FBA_REPORTS) {
			if (int(($so->get('.fba_reportpermissions'))& $SYNDICATION::AMAZONREPORTS::FBA_REPORTS{$report})>0) {
				push @REPORTS, $report;
				}
			}
	   }
	else {
		## The merchant has not selected any reports from the Amazon reports panel so there is no need to go any further
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s has not sellected any reports from the Amazon Reports panel",$USERNAME));
		}

	return ($lm,@REPORTS);
	}	


### request_report
#		The RequestReport API operation creates a report request. Amazon MWS processes the report request and when the report is ready, 
#		sets the status of the report request to _DONE_. Reports are retained for 90 days.
#
#		Once the report has been created the reportid will be included in the list of available reports returned by the getReportList 
#		call (called by subroutine 'run').
#
#		Note: see SYNDICATION::AMAZONREPORTS::FBA_REPORTS and SYNDICATION::AMAZONREPORTS::AMAZON_REPORTS for accepted TYPEs

sub request_report {
	my ($so,$TYPE,%options) = @_;

	my $USERNAME = $so->username();

	my ($lm) = $options{'*LM'};
	if (not defined $lm) {
		$lm = LISTING::MSGS->new($USERNAME,logfile=>"~/amazon-reports-%YYYYMM%.log",'stderr'=>0);
		}

	$lm->pooshmsg("INFO|+Start RequestReport");

	my $PRT = $options{'PRT'};
	if ($PRT eq '')	{
		$PRT = undef;
		}
	my ($userref) = &AMAZON3::fetch_userprt($USERNAME,$PRT);
	my $date = &ZTOOLKIT::pretty_date(time(),1);
	my ($udbh) = &DBINFO::db_user_connect($USERNAME);

	if ($userref->{'AMAZON_MARKETPLACEID'} eq '') {
		## w/o the marketplaceid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missing AMAZON_MARKETPLACEID",$USERNAME));
		}
	elsif ($userref->{'AMAZON_MERCHANTID'} eq '') {
		## w/o the merchantid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missising AMAZON_MERCHANTID",$USERNAME));
		}

	my $agent = new LWP::UserAgent;
	$agent->agent('Zoovy/just-testing1 (Language=Perl/v5.8.6)');

	my @REQUESTS = ();
	my %HEADERS = ();
	my $START_TS = time();			## on success save the time we started, not the time we finished.
	my $REQUESTID = '';


	if (not $lm->can_proceed()) {
		## something has already gone wrong
		}
	else {
		$HEADERS{'Action'} = 'RequestReport';
		$HEADERS{'ReportType'} = $TYPE;
		
		push @REQUESTS, \%HEADERS;
		}
	
	my $API_FAILURES = 0;
	while (my $headers = shift @REQUESTS) {
		next if (not $lm->can_proceed());		## fatal errors will stop us!

		my ($request_url, $head) = &mws_headers("/",$userref,\%HEADERS); 	
				#	the reports API doesn't use a uri hence why we only pass "/". 
				#	mws_headers accepts a uri because it is accessed from many other scripts that require one. 

				#	we could just change mws_headers to default to "/" if no uri is passed but I want to force the scripts to specify exactly what
				#	they want to use so that we know that uri hasn't just been accidently omitted from the call.

		my $request = HTTP::Request->new('POST',$request_url,$head);
		my $response = $agent->request($request);

		if ($response->code() == 400) {
			## 400 = this usually means account was suspended or password is wrong.
			$lm->pooshmsg("FAIL-FATAL|+HTTP 400 response code (account was suspended or password is wrong)");
			}
		elsif (not $response->is_success()) {
			## High level API Failure (this could mean that either Amazon or ourselves are down)
			$lm->pooshmsg(sprintf("%s|+API ERROR[%d] %s",(($API_FAILURES<3)?'WARN':'ERROR'),$API_FAILURES,$response->content()));
			$API_FAILURES++;
			if ($API_FAILURES < 3) {
				## lets make this request again!
				unshift @REQUESTS, $headers;
				}
			}
		else {
			my $raw_xml_response = $response->content();
			my ($sh) = IO::String->new(\$raw_xml_response);

			my ($msg) = XML::SAX::Simple::XMLin($sh,ForceArray=>1);

			&stripNamespace($msg);	
			my $PRETTY_PARSEDXML_RESPONSE = $msg;

			if ($PRETTY_PARSEDXML_RESPONSE->{'RequestReportResult'}[0]->{'ReportRequestInfo'}[0]->{'ReportRequestId'}[0] ne '') {
				$REQUESTID = $PRETTY_PARSEDXML_RESPONSE->{'RequestReportResult'}[0]->{'ReportRequestInfo'}[0]->{'ReportRequestId'}[0];
				$lm->pooshmsg(sprintf("INFO|+The request for report type: %s was successfull. The Request ID returned by Amazon is %d",$TYPE, $REQUESTID));
				}
			else {
				$lm->pooshmsg(sprintf("INFO|+The request for report type: %s was successfull. but no Request ID was returned by Amazon",$TYPE));
				}
			}
		}
	return ($lm, $REQUESTID);
	}
	

### run
#		1. 	Returns a list of available reports using GetReportList.
#		2. 	Calls the get_report subroutine to pull the report from Amazon.
#		3. 	Once all reports have been processed, calls the ack_reports subroutine to acknowledge the reports. It does this so that their ids are not returned next time
#			we call GetReportList.
#
#		GetReportList
#			The GetReportList operation returns a list of reports that have been created in the previous 90 days that match the query parameters.
#			A maximum of 100 results can be returned in one request. If there are additional results to return, HasNext is returned, set to true 
#			in the response. To retrieve all the results, we pass the value of the NextToken parameter to the GetReportListByNextToken operation 
#			iteratively until HasNext is returned, set to false.  

sub run {
	my ($so,%options) = @_;

	my $USERNAME = $so->username();

	my $PRT = $so->prt;
	if ($PRT eq '')	{
		$PRT = undef;
		}
	my ($userref) = &AMAZON3::fetch_userprt($USERNAME,$PRT);
	my $date = &ZTOOLKIT::pretty_date(time(),1);
	my ($udbh) = &DBINFO::db_user_connect($USERNAME);

	my ($lm) = LISTING::MSGS->new($USERNAME,logfile=>"~/amazon-reports-%YYYYMM%.log",'stderr'=>0);

	if ($userref->{'AMAZON_MARKETPLACEID'} eq '') {
		## w/o the marketplaceid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missing AMAZON_MARKETPLACEID",$USERNAME));
		}
	elsif ($userref->{'AMAZON_MERCHANTID'} eq '') {
		## w/o the merchantid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missising AMAZON_MERCHANTID",$USERNAME));
		}

	my $agent = new LWP::UserAgent;
	$agent->agent('Zoovy/just-testing1 (Language=Perl/v5.8.6)');

	my @REQUESTS = ();
	my %AVAILABLE_REPORTS = ();
	my $START_TS = time();	
	$lm->pooshmsg("INFO|+Start getReportList");

	my ($lm, @REPORTS) = &get_permissions($so,$lm);

	if (not $lm->can_proceed()) {
		## something has already gone wrong
		}
	else {
		my %HEADERS = ();
		$HEADERS{'Action'} = 'GetReportList';
		$HEADERS{'Acknowledged'} = 'false';

		my $i = 1;
		foreach my $report(@REPORTS) { 
			$HEADERS{sprintf('ReportTypeList.Type.%d',$i++)} = $report;
			}
		push @REQUESTS, \%HEADERS;
		}

	my $API_FAILURES = 0;
	while (my $headers = shift @REQUESTS) {
		next if (not $lm->can_proceed());		## fatal errors will stop us!

		my ($request_url, $head) = &mws_headers("/",$userref,$headers);

		my $request = HTTP::Request->new('POST',$request_url,$head);
		my $response = $agent->request($request);

		if ($response->code() == 400) {
			## 400 = this means account was suspended, password is wrong or sometimes that the nextToken was invalid.
			## this is DEFINITELY not a retry condition.
			$lm->pooshmsg("FAIL-FATAL|+HTTP 400 response code (account was suspended, password is wrong or on the odd occasion the nextToken is invalid)");
			}
		elsif (not $response->is_success()) {
			## High level API Failure (this could mean that either Amazon or ourselves are down)
			$lm->pooshmsg(sprintf("%s|+API ERROR[%d] %s",(($API_FAILURES<3)?'WARN':'ERROR'),$API_FAILURES,$response->content()));
			$API_FAILURES++;
			if ($API_FAILURES < 3) {
				## lets make this request again!
				unshift @REQUESTS, $headers;
				}
			}
		else {
			## we did not receive an api error so set $xml
			my $raw_xml_response = $response->content();
			my ($sh) = IO::String->new(\$raw_xml_response);
			open F, ">/dev/shm/amz_rec.raw_xml_response";
			print F $raw_xml_response;
			close F;

			my ($msgs) = XML::SAX::Simple::XMLin($sh,ForceArray=>1);

			&stripNamespace($msgs);	
			my $PRETTY_PARSEDXML_RESPONSE = $msgs;

			## set $TOP_LEVEL_ELEMENT - eg  GetReportList or  GetReportListByNextTokenResult
			my $TOP_LEVEL_ELEMENT = $headers->{'Action'}."Result";	
			if (($PRETTY_PARSEDXML_RESPONSE->{$TOP_LEVEL_ELEMENT}[0]->{'NextToken'}[0] ne '') && ($PRETTY_PARSEDXML_RESPONSE->{$TOP_LEVEL_ELEMENT}[0]->{'HasNext'}[0] eq 'true')) {
				## if NextToken is returned, Amazon have not yet retuned the entire response so we need to use the NextToken to ask for more
				##		- We check both HasNext and NextToken because sometimes the NextToken has data even if there is no next page. 
 
				my $NEXT_TOKEN = $PRETTY_PARSEDXML_RESPONSE->{$TOP_LEVEL_ELEMENT}[0]->{'NextToken'}[0];
				if ($NEXT_TOKEN ne '') {
					## append this next token to the front of the list of REQUESTS
					push @REQUESTS, { 'Action'=>'GetReportListByNextToken', 'NextToken'=>$NEXT_TOKEN };
					}
				}

			if (defined $PRETTY_PARSEDXML_RESPONSE->{$TOP_LEVEL_ELEMENT}[0]->{'ReportInfo'}) {
				foreach my $msg (@{$PRETTY_PARSEDXML_RESPONSE->{$TOP_LEVEL_ELEMENT}[0]->{'ReportInfo'}}) {
					my ($node) = ZTOOLKIT::XMLUTIL::SXMLflatten($msg);

					## EXAMPLE GetReportList $node:

					#'.ReportId' => '13345780213',
					#'.ReportType' => '_GET_SELLER_FEEDBACK_DATA_',
					#'.Acknowledged' => 'false',
					#'.ReportRequestId' => '8349428512',
					#'.AvailableDate' => '2013-11-05T02:24:52+00:00'

					$AVAILABLE_REPORTS{ $node->{'.ReportId'} } = $node;
					}
				}
			}
		}
	
	## SANITY: at this point %AVAILABLE_REPORTS is populated
	##		Lets go and get them from Amazon.


	if ($lm->can_proceed()) {
		foreach my $report (sort keys %AVAILABLE_REPORTS) {
			my $REF = $AVAILABLE_REPORTS{$report};

			## check if report has already been created
			my $pstmt = "select count(1) from AMAZON_REPORTS where REPORTID = ".$udbh->quote($REF->{'.ReportId'});
			print STDERR $pstmt."\n";

			my $sth = $udbh->prepare($pstmt);
			$sth->execute();
			my ($count) = $sth->fetchrow();
			$sth->finish;

			print Dumper($count);
			if (int($count)>0) {
				## The report already exists so let's acknowledge it so Amazon doesn't send it again.
				$lm->pooshmsg("INFO|+Report ".$REF->{'.ReportId'}." already exists. Lets acknowledge it");
				&ack_reports($so,$userref,$lm,$REF);
            }
			else {
				&get_report($so,$userref,$lm,'REPORTREF'=>$REF);
				}
			}
		}
	if (not $lm->can_proceed()) {
		$lm->pooshmsg("WARN|+End getReportList");
		}
	elsif (scalar(keys %AVAILABLE_REPORTS) == 0) {
		$lm->pooshmsg("WARN|+no report lists returned from Amazon");
		}
	else {
		$lm->pooshmsg(sprintf("SUCCESS|+End getReportList %d records, took %d seconds",(scalar keys %AVAILABLE_REPORTS),(time()-$START_TS)));
		}

	DBINFO::db_user_close();
	&ack_reports($so,$userref,$lm); # We need to acknowledge all of the documents that we've received so Amazon don't give them to us next time we run this script.
	}

### get_report
#
#	Returns the contents of a report and saves it as a tsv file in PRIVATE_FILES.
#	Requires either a REPORTREF or (a REPORTID and TYPE - the absence of TYPE isn't a STOP error but TYPE is highly recommended)
#
sub get_report {
	my ($so,$userref,$lm,%options) = @_;

	my ($USERNAME) = $so->username();
	my ($MID) = &ZOOVY::resolve_mid($USERNAME);
	my ($PRT) = $userref->{'PRT'};

	if ($userref->{'AMAZON_MARKETPLACEID'} eq '') {
		## w/o the marketplaceid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missing AMAZON_MARKETPLACEID",$USERNAME));
		}
	elsif ($userref->{'AMAZON_MERCHANTID'} eq '') {
		## w/o the merchantid, MWS will not function
		$lm->pooshmsg(sprintf("STOP|+MERCHANT:%s is missising AMAZON_MERCHANTID",$USERNAME));
		}

	my $REPORTREF = $options{'REPORTREF'};

	my $REPORTID = ''; 
	if ($options{'REPORTID'} ne '') {
		$REPORTID = $options{'REPORTID'};
		}
	elsif (defined $REPORTREF->{'.ReportId'}) {
		$REPORTID = $REPORTREF->{'.ReportId'}
		}
	else {
		$lm->pooshmsg('STOP|+REPORTID is blank. We cant get a report a report without a reportid');
		}

	my $TYPE = ''; 
	if ($options{'TYPE'} ne '') {
		$TYPE = $options{'TYPE'};
		}
	elsif (defined $REPORTREF->{'.ReportType'}) {
		$TYPE = $REPORTREF->{'.ReportType'}
		}
	else {
		## TYPE is not required but is preferred because it's used in the filename
		$lm->pooshmsg('INFO|+TYPE is blank. TYPE is strongly recommended');
		}
	my $date = &ZTOOLKIT::pretty_date(time(),1);
	my ($udbh) = &DBINFO::db_user_connect($USERNAME);
	my $agent = new LWP::UserAgent;
	$agent->agent('Zoovy/just-testing1 (Language=Perl/v5.8.6)');

	my @REQUESTS = ();
	my %HEADERS = ();
	my $START_TS = time();	
	
	$lm->pooshmsg("INFO|+Start get_report for REPORTID ". $REPORTID);

	if (not $lm->can_proceed()) {
		## something has already gone wrong
		}
	else {
		$HEADERS{'Action'} = 'GetReport';
		$HEADERS{'ReportId'} = $REPORTID;
		
		push @REQUESTS, \%HEADERS;
		}
	
	my $API_FAILURES = 0;
	while (my $headers = shift @REQUESTS) {
		next if (not $lm->can_proceed());		## fatal errors will stop us!

		my ($request_url, $head) = &mws_headers("/",$userref,\%HEADERS); 
		
		my $request = HTTP::Request->new('POST',$request_url,$head);
		my $response = $agent->request($request);
		my $report_md5 = '';
		my $header_md5 = '';
		my $report_content = '';

		if ($response->code() == 400) {
			## 400 = this probably means account was suspended or password is wrong.
			$lm->pooshmsg("FAIL-FATAL|+HTTP 400 response code (account was suspended or password is wrong)");
			}
		elsif (not $response->is_success()) {
			## High level API Failure (this could mean that either Amazon or ourselves are down)
			$lm->pooshmsg(sprintf("%s|+API ERROR[%d] %s",(($API_FAILURES<3)?'WARN':'ERROR'),$API_FAILURES,$response->content()));
			$API_FAILURES++;
			if ($API_FAILURES < 3) {
				## lets make this request again!
				unshift @REQUESTS, $headers;
				}
			}
		else {
			$report_content = $response->content();

			print Dumper($report_content);

			#	Apparently Amazon sometimes sends report content that does not belong to the requested report. To get around this we need to compute the 
			#	MD5 hash of the HTTP body and compare that with the returned Content- MD5 header value. If they do not match, it means the body was 
			#	corrupted during transmission. If the report is corrupted, we need to discard the result and automatically retry the request three 
			#	more times.

			$header_md5 = $response->header('content-md5');
			$report_md5 = &Digest::MD5::md5_base64($report_content);
			$report_md5 .= "==";		## an amazon md5 always ends with '=='.  this is officially duct-tape but without it the  md5s don't match
		
			my $MD5_ERRORS = 0;
			if ($header_md5 eq $report_md5) {
				# the md5s match so we know we have the correct report

				my $FILENAME = "/tmp/amz-".$USERNAME."-".$REPORTID."-".$TYPE."-report.tsv";	
				open F, ">$FILENAME"; print F $report_content; close F;

				## write to merchant's PRIVATE dir
				require LUSER::FILES;
				my ($lf) = LUSER::FILES->new($USERNAME,'app'=>'AMAZON');
				my $guid = undef;
				if (defined $lf) {
					($guid) = $lf->add(
						'*lm'=>$lm,
		  	  			file=>$FILENAME,
						title=>"Amazon ".$TYPE." Report: Report ID: ".$REPORTID,
						type=>'Report',
	  		 			overwrite=>1,
						createdby=>'*AMAZON',
						meta=>{'DSTCODE'=>'AMZ','PROFILE'=>$PRT,'TYPE'=>'REPORT'},
						);
					}
				print "FILENAME: $FILENAME\n";
				$lm->pooshmsg("INFO|+filename is $FILENAME.");

				}
			else {
				$MD5_ERRORS++;
				if ($MD5_ERRORS < 4) {
					#	Amazon suggests we should try 4 times.
					unshift @REQUESTS, $headers;
					$lm->pooshmsg(sprintf("WARN|+Amazon have sent the wrong report for Report ID %s, %d time/s. we'll try again",$REPORTID,$MD5_ERRORS));
					}
				else {
					#MD5s have been mismatched 4 times.  
					$lm->pooshmsg(sprintf("STOP|+Amazon sent the wrong report for Report ID %s. Amazon will need to be contacted ",$REPORTID));
					}
				}
			}
		}

	if (not $lm->can_proceed()) {
		# something has already gone wrong
		}
	else {
		## SANITY - at this point the report should have been saved to the merchants PRIVATE_FILES where JT can make it look magnificent.
		
		#  lets also save the report reference to the AMAZON_REPORTS. Once in the table the ack_report subroutine will acknowledge receipt of the doc.
		my ($pstmt) = &DBINFO::insert($udbh,'AMAZON_REPORTS',{
			MID=>$MID,
			CREATED_GMT=>time(),
			PRT=>$PRT,
			TYPE=>$REPORTREF->{'.ReportType'},
			REPORTID=>$REPORTREF->{'.ReportId'},
			START_DATE=>time(),
			END_DATE=>time(),
			},sql=>1,'verb'=>'insert');

		print STDERR $pstmt."\n";
		$udbh->do($pstmt);
		$lm->pooshmsg("INFO|+Report ".$REPORTREF->{'.ReportId'}." created.");
		$lm->pooshmsg("INFO|+pstmt is $pstmt.");
		}		
	DBINFO::db_user_close();
	}

## ack_reports
## -	The UpdateReportAcknowledgements operation is a request that updates the acknowledged status of one or more reports. 
#	-	Although this call is optional from Amazon's standpoint, if we don't acknowledge the reports we receive Amazon will continue to send them.
#		When we request a list of available reports using the GetReportList call we only request reports that have not been acknowledged.
#
sub ack_reports {
	my ($so,$userref,$lm,$REPORTREF) = @_;

	my ($USERNAME) = $so->username();
	my ($PRT) = $userref->{'PRT'};
	my $date = &ZTOOLKIT::pretty_date(time(),1);
	my ($udbh) = &DBINFO::db_user_connect($USERNAME);
	my $agent = new LWP::UserAgent;
	$agent->agent('Zoovy/just-testing1 (Language=Perl/v5.8.6)');

	my @REQUESTS = ();
	my @REPORTS = ();
	my $START_TS = time();			## on success save the time we started, not the time we finished.
	my %ACKED_REPORTS = ();

	$lm->pooshmsg("INFO|+Start ack_reports");

	if (not $lm->can_proceed()) {
		#something has already gone wrong
		}
	elsif ($REPORTREF ne '') {
		#	we're dealing with a request made to ack a specific report rather than all reports
		#		- 	this should not be necessary but if the order has some how been incorrectly marked as acked in AMAZON_REPORTS or Amazon failed to set the 
		#			report to acknowledged at their end, we need to re-ack it.
		#		- 	eg subroutine 'run' calls ack_reports when it receives a report ID from GetReportList that already exists in the AMAZON_REPORTS table.     
		push @REPORTS, $REPORTREF->{'.ReportId'};
		}
	else {
		# check for reports to ack 
		my $pstmt = 'select REPORTID from AMAZON_REPORTS where ACK_GMT=0';
		my $sth = $udbh->prepare($pstmt);
		$sth->execute();
		while (my $report = $sth->fetchrow()) {
			push @REPORTS, $report;
			}
		$sth->finish;

		if (not $lm->can_proceed()) {
			## something has already gone wrong
			}
		elsif (@REPORTS ne '') {
			foreach my $batch (@{&ZTOOLKIT::batchify(\@REPORTS,100)}) {
				my %HEADERS = ();			
				$HEADERS{'Action'} = 'UpdateReportAcknowledgements';
				$HEADERS{'Acknowledged'} = 'true';
				my $i = 1;
				foreach my $reportid (@{$batch}) {
					$HEADERS{sprintf('ReportIdList.Id.%d',$i++)} = $reportid;
					}	
				push @REQUESTS, \%HEADERS;
				}
			}
  		else {
  			$lm->pooshmsg("ISE|+no reports to acknowledge");
			}
		}


	## SANITY - @REPORTS should now contain all calls we're going to make
	my $API_FAILURES = 0;
	while (my $headers = shift @REQUESTS) {
		next if (not $lm->can_proceed());		## fatal errors will stop us!

		my ($request_url, $head) = &mws_headers("/",$userref,$headers); #the reports API doesn't seem to use a uri 

		my $request = HTTP::Request->new('POST',$request_url,$head);
		my $response = $agent->request($request);

		if ($response->code() == 400) {
			print Dumper($response);
			print Dumper($headers);
			## 400 = this probably means the account was suspended or password is wrong.
			## this is DEFINITELY not a retry condition.
			$lm->pooshmsg("FAIL-FATAL|+HTTP 400 response code (account was suspended or password is wrong)");
			}
		elsif (not $response->is_success()) {
			## High level API Failure (this could mean that either Amazon or ourselves are down)
			$lm->pooshmsg(sprintf("%s|+API ERROR[%d] %s",(($API_FAILURES<3)?'WARN':'ERROR'),$API_FAILURES,$response->content()));
			$API_FAILURES++;
			if ($API_FAILURES < 3) {
				## lets make this request again!
				unshift @REQUESTS, $headers;
				}
			}
		else {
			# success!!!
			## we did not receive an api error so set $xml
			my $raw_xml_response = $response->content();
			my ($sh) = IO::String->new(\$raw_xml_response);
			open F, ">/dev/shm/amz_rec.raw_xml_response";
			print F $raw_xml_response;
			close F;

			my ($msgs) = XML::SAX::Simple::XMLin($sh,ForceArray=>1);

			&stripNamespace($msgs);	
			my $PRETTY_PARSEDXML_RESPONSE = $msgs;

			if (defined $PRETTY_PARSEDXML_RESPONSE->{'UpdateReportAcknowledgementsResult'}[0]->{'ReportInfo'}) {
				foreach my $msg (@{$PRETTY_PARSEDXML_RESPONSE->{'UpdateReportAcknowledgementsResult'}[0]->{'ReportInfo'}}) {
					my ($node) = ZTOOLKIT::XMLUTIL::SXMLflatten($msg);

					## EXAMPLE 'UpdateReportAcknowledgements' $node:

					#'.ReportId' => '841997483',
					#'.ReportType' => '_GET_SELLER_FEEDBACK_DATA_',
					#'.Acknowledged' => 'true',
					#'.ReportRequestId' => '8349428512',
					#'.AvailableDate' => '2013-12-19T02:24:52+00:00',
					#'.AcknowledgedDate' => '2013-12-20T02:10:41+00:00'

					$ACKED_REPORTS{ $node->{'.ReportRequestId'} } = $node;
					}
				}
			}
		

		print Dumper(%ACKED_REPORTS);
		if (not $lm->can_proceed()) {
			# something has already gone wrong
			}
		else {
			foreach my $report (sort keys %ACKED_REPORTS) {
				my $REF = $ACKED_REPORTS{$report};
				if ($REF->{'.Acknowledged'} eq 'true') {
					my $pstmt = "update AMAZON_REPORTS set ACK_GMT=now() where REPORTID=".$udbh->quote($REF->{'.ReportId'});
					my $sth = $udbh->prepare($pstmt);
					$sth->execute();
					$sth->finish();
					}
				else {
					#for some reason Amazon didn't accept the acknowledgement
					$lm->pooshmsg(sprintf("INFO|+Amazon did not accept the acknowledgement for Report ID:%s",$REF->{'.ReportId'}));
					}
				}
			}
		}
	$lm->pooshmsg("INFO|+Finish ack_reports");

	DBINFO::db_user_close();
	}

# creates the url and header for a post to amazon.
sub mws_headers {
	my ($request_uri, $userref, $action_paramref) = @_;
	my $XML = $action_paramref->{'XML'};

	## 1. define credentials
	my $AMZ_MARKETPLACEID = $userref->{'AMAZON_MARKETPLACEID'};
	my $AMZ_MERCHANTID = $userref->{'AMAZON_MERCHANTID'};
	my $host = "mws.amazonservices.com";
	my $sk = $userref->{'SECRET_KEY'};;
	my $awskey = $userref->{'AWSKEY'};;
	my $TS = AMAZON3::amztime(time()+(8*3600));
	my $md5 = &Digest::MD5::md5_base64($XML);
	$md5 .= "==";		## this is officially duct-tape, run w/o and md5's dont match

	my %params = (
		'AWSAccessKeyId'=>$awskey,
		'MarketplaceId'=>$AMZ_MARKETPLACEID,
		'SellerId'=>$AMZ_MERCHANTID,
		'SignatureVersion'=>2,
		'SignatureMethod'=>'HmacSHA1',
		'Timestamp'=>$TS,
		);

	## populate params with actions passed in $action_paramref 
	## ie Action, PreportType, 
	foreach my $action_param (keys %{$action_paramref}) {
		if ($action_param ne 'XML') {
			$params{$action_param} = $action_paramref->{$action_param};
			}
		}

	## 2. create header
	my $head = HTTP::Headers->new();
	$head->header('Content-Type'=>'text/xml');
	$head->header('Host',$host);	
	$head->header('Content-MD5',$md5);

	## 3. create query string
	my $query_string = '';
	foreach my $k (sort keys %params) {
		$query_string .= URI::Escape::uri_escape_utf8($k).'='.URI::Escape::uri_escape_utf8($params{$k}).'&';
		}
	$query_string = substr($query_string,0,-1);	# strip trailing &

	## 4. create string to sign
	my $url = "https://mws.amazonaws.com";
	my $data = 'POST';
	$data .= "\n";
	$data .= $host;
	$data .= "\n";
	$data .= $request_uri;
	$data .= "\n";
	$data .= $query_string;

	## 5. create digest by calculating HMAC, convert to base64
	my $digest = Digest::HMAC_SHA1::hmac_sha1($data,$sk);
	$digest = MIME::Base64::encode_base64($digest);
	$digest =~ s/[\n\r]+//gs;

	## 6. POST contents to MWS
	my %sig = ('Signature'=>$digest);
	my $request_url = $url.$request_uri."?".$query_string."&".&AMAZON3::build_mws_params(\%sig);

	return($request_url, $head);	
	}

## STRIP NAMESPACE: stripNamespace rewrites the sax xml without namespaces e.g.:
# 	original: {https://mws.amazonservices.com/}ResponseMetadata'=>{..}
# 	into: 'ResponseMetadata'=>{}
##  the xml response looks very different (but much more manageable after stripNamespace)
sub stripNamespace {
	my ($ref) = @_;

	if (ref($ref) eq 'HASH') {
		foreach my $k (keys %{$ref}) {
			if (ref($ref->{$k}) ne '') { 
				&stripNamespace($ref->{$k}); 
				}
			if ($k =~ /^\{(urn|http|https)\:.*?\}(.*?)$/) {
				$ref->{$2} = $ref->{$k};
				delete $ref->{$k};
				}
			}
		}
	elsif (ref($ref) eq 'ARRAY') {
		foreach my $x (@{$ref}) {
			&stripNamespace($x);
			}
		}
	}

## AVAILABLE AMAZON REPORTS BITWISE LOOKUP TABLE
##		-	This is not the most obvious way to do this but the table may also be accessed from the UI.

%SYNDICATION::AMAZONREPORTS::AMAZON_REPORTS = (
	##### LISTINGS REPORTS

	## Open Listings Report ("Inventory Report")
	# 		- 	Tab-delimited flat file open listings report that contains the SKU, ASIN, Price, and Quantity fields. For Marketplace and Seller Central sellers.
	'_GET_FLAT_FILE_OPEN_LISTINGS_DATA_'=>1, 

	## Open Listings Report
	#		-	Tab-delimited flat file open listings report.
	'_GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT_'=>2,

	## Merchant Listings Report ("Active Listings Report")
	#		-	Tab-delimited flat file detailed active listings report. For Marketplace and Seller Central sellers.
	'_GET_MERCHANT_LISTINGS_DATA_'=>4,

	##	Merchant Listings Lite Report ("Open Listings Lite Report")
	#		-	Tab-delimited flat file active listings report that contains only the SKU, ASIN, Price, and Quantity fields for items that have a quantity 
	#			greater than zero. For Marketplace and Seller Central sellers.
	'_GET_MERCHANT_LISTINGS_DATA_LITE_'=>8,

	## Merchant Listings Liter Report ("Open Listings Liter Report")
	#		-	Tab-delimited flat file active listings report that contains only the SKU and Quantity fields for items that have a quantity greater than 
	#			zero. For Marketplace and Seller Central sellers.
	'_GET_MERCHANT_LISTINGS_DATA_LITER_'=>16,

	##	Sold Listings Report
	#		-	Tab-delimited flat file sold listings report that contains items sold on Amazon's retail website. For Marketplace and Seller Central sellers.
	'_GET_CONVERGED_FLAT_FILE_SOLD_LISTINGS_DATA_'=>32,

	##	Canceled Listings Report
	#		-	Tab-delimited flat file canceled listings report. For Marketplace and Seller Central sellers.
	'_GET_MERCHANT_CANCELLED_LISTINGS_DATA_'=>64,

	##	Quality Listing Report ("Listing Quality and Suppressed Listing Report")
	#		-	Tab-delimited flat file quality listing report that contains the following fields: sku, product-name, asin, field-name, alert-type, 
	#			current-value, last-updated, alert-name, and status. For Marketplace and Seller Central sellers.
	'_GET_MERCHANT_LISTINGS_DEFECT_DATA_'=>128,


	##### ORDER TRACKING REPORTS
	##		These order tracking reports are available in North America (NA) and Europe (EU), and can be used by all Amazon sellers. These reports 
	#		return all orders, regardless of fulfillment channel or shipment status. These reports are intended for order tracking, not to drive 
	#		your fulfillment process, as the reports do not include customer-identifying information and scheduling is not supported. Also note that 
	#		for MFN orders, item price is not shown for orders in a "pending" state.

	##	Flat File Orders By Last Update Report
	#		-	Tab-delimited flat file report that shows all orders updated in the specified period. Cannot be scheduled. For all sellers.
	'_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_'=>256,

	##	Flat File Orders By Order Date Report
	#		-	Tab-delimited flat file report that shows all orders that were placed in the specified period. Cannot be scheduled. For all sellers.
	'_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'=>512,


	#####	SETTLEMENT REPORTS
	##			NOTE:: SETTLEMENT They are automatically scheduled by Amazon.

	##	Flat File Settlement Report
	#		-	Tab-delimited flat file settlement report that is automatically scheduled by Amazon; it cannot be requested through RequestReport. 
	#			For all sellers.
	'_GET_FLAT_FILE_PAYMENT_SETTLEMENT_DATA_'=>1024,

	##	Flat File V2 Settlement Report
	#		-	Tab-delimited flat file alternate version of the Flat File Settlement Report. Price columns are condensed into three general purpose 
	#			columns: amounttype, amountdescription, and amount. This report is automatically scheduled by Amazon for FBA sellers; it cannot be 
	#			requested through RequestReport. For Seller Central sellers only.
	'_GET_ALT_FLAT_FILE_PAYMENT_SETTLEMENT_DATA_'=>2048,


	#####	AMAZON PRODUCT ADS REPORTS

	##	Product Ads Listings Report
	#		-	Tab-delimited flat file detailed active listings report. For Amazon Product Ads sellers only.
	'_GET_NEMO_MERCHANT_LISTINGS_DATA_'=>4096,

	#####	SALES TAX REPORTS
	##			NOTE: SALES REPORTS CAN NOT BE REQUESTED

	##	Sales Tax Report
	#		-	Tab-delimited flat file for tax-enabled US sellers. Content updated daily. This report cannot be requested or scheduled. You must 
	#			generate the report from the Tax Document Library in Seller Central. For Marketplace and Seller Central sellers.
	'_GET_FLAT_FILE_SALES_TAX_DATA_'=>8192,
	);


## AVAILABLE FBA REPORTS BITWISE LOOKUP TABLE
%SYNDICATION::AMAZONREPORTS::FBA_REPORTS = (

	##### FBA REPORTS
	#			There are limits to how often Amazon will generate FBA reports. These limits depend on whether an FBA report is a near real-time report 
	#			or a daily report. See the following table to see which FBA reports are near real-time and which are daily.
	#			A near real-time FBA report is generated no more than once every 30 minutes. This means that after a near real-time FBA report is generated 
	#			following your report request, a 30-minute waiting period must pass before Amazon will generate an updated version of that report. 
	#			Note that the four "All Orders" reports are not subject to this limitation.
	#			A daily FBA report is generated no more than once every four hours. This means that after a daily FBA report is generated following your 
	#			report request, a four-hour waiting period must pass before Amazon will generate an updated version of that report.

	##### FBA SALES REPORTS

	##	FBA Amazon Fulfilled Shipments Report
	#		-	Daily Report
	#		-	Tab-delimited flat file. Contains detailed order/shipment/item information including price, address, and tracking data. You can request 
	#			up to one month of data in a single report. Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_'=>1,

	##	Flat File All Orders Report by Last Update
	#		-	Tab-delimited flat file. Returns all orders updated in the specified date range regardless of fulfillment channel or shipment status. 
	#			This report is intended for order tracking, not to drive your fulfillment process; it does not include customer identifying information 
	#			and scheduling is not supported. For all sellers.
	'_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_'=>2,

	##	Flat File All Orders Report by Order Date
	#		-	Tab-delimited flat file. Returns all orders placed in the specified date range regardless of fulfillment channel or shipment status. 
	#			This report is intended for order tracking, not to drive your fulfillment process; it does not include customer identifying information 
	#			and scheduling is not supported. For all sellers.
	'_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'=>4,

	##	FBA Customer Shipment Sales Report
	#		-	Tab-delimited flat file. Contains condensed item level data on shipped FBA customer orders including price, quantity, and ship to location. 
	#			Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA_'=>8,

	##	FBA Promotions Report
	#		- 	Updated Daily
	#		-	Tab-delimited flat file. Contains promotions applied to FBA customer orders sold through Amazon; e.g. Super Saver Shipping. 
	#			Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA_'=>16,

	##	FBA Customer Taxes
	#		-	Tab-delimited flat file for tax-enabled US sellers. This report contains data through February 28, 2013. All new transaction data can be found 
	#			in the Sales Tax Report. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CUSTOMER_TAXES_DATA_'=>32,


	##### FBA INVENTORY REPORTS

	##	FBA Received Inventory Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains inventory that has completed the receive process at Amazon's fulfillment centers. Content updated daily. 
	#			For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_INVENTORY_RECEIPTS_DATA_'=>64,

	##	FBA Inventory Event Detail Report
	#		-	Updated daily
	#		-	Tab-delimited flat file. Contains history of inventory events (e.g. receipts, shipments, adjustments etc.) by SKU and Fulfillment Center. 
	#			Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_INVENTORY_SUMMARY_DATA_'=>128,

	##	FBA Inventory Adjustments Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains corrections and updates to your inventory in response to issues such as damage, loss, receiving 
	#			discrepancies, etc. Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA_'=>256,

	##	FBA Inventory Health Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains information about inventory age, condition, sales volume, weeks of cover, and price. 
	#			Content updated daily. For FBA Sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA_'=>512,

	##	FBA Manage Inventory
	#		-	Updated in real time.
	#		-	Tab-delimited flat file. Contains current details of active (not archived) inventory including condition, quantity and volume. Content 
	#			updated in near real-time. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA_'=>1024,

	##	FBA Manage Inventory - Archived
	#		-	Updated in real time
	#		-	Tab-delimited flat file. Contains current details of all (including archived) inventory including condition, quantity and volume. 
	#			Content updated in near real-time. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_MYI_ALL_INVENTORY_DATA_'=>2048,

	##	FBA Cross-Border Inventory Movement Report
	#		-	Updated Daily
	#		-	Tab delimited flat file. Contains historical data of shipments that crossed country borders. These could be export shipments or 
	#			shipments using Amazon's European Fulfillment Network (note that Amazon's European Fulfillment Network is for Seller Central sellers only). 
	#			Content updated daily. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CROSS_BORDER_INVENTORY_MOVEMENT_DATA_'=>4096,

	##	FBA Inbound Performance Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains inbound shipment problems by product and shipment. Content updated daily. For FBA sellers only. For 
	#			Marketplace and Seller Central.
	'_GET_FBA_FULFILLMENT_INBOUND_NONCOMPLIANCE_DATA_'=>8192,

	##	FBA Hazmat Status Change Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Indicates the current hazmat status of items in your inventory, which determines whether or not an item can be 
	#			shipped to an Amazon fulfillment center. Content updated daily. For FBA sellers in NA only. For Marketplace and Seller Central sellers.
	'_GET_FBA_HAZMAT_STATUS_CHANGE_DATA_'=>16384,


	#####	FBA PAYMENTS REPORTS

	##	FBA Fee Preview Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains the estimated Amazon Selling and Fulfillment Fees for your current FBA inventory. The data in the report 
	#			may be up to 72 hours old. Content updated daily. For FBA sellers in the US and EU only. For Marketplace and Seller Central sellers.
	'_GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA_'=>32768,

	##	FBA Reimbursements Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains itemized details of your inventory reimbursements including the reason for the reimbursement. Content 
	#			updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_REIMBURSEMENTS_DATA_'=>65536,


	#####	FBA CUSTOMER CONCESSIONS REPORTS

	##	FBA Returns Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains customer returned items received at an Amazon fulfillment center, including Return Reason and Disposition. 
	#			Content updated daily. For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_'=>131072,

	##	FBA Replacements Report
	#		-	Updated Daily
	#		-	Tab-delimited flat file. Contains replacements that have been issued to customers for completed orders. Content updated daily. 
	#			For FBA sellers only. For Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA_'=>262144,

	#####	FBA REMOVALS REPORTS

	##	FBA Recommended Removal Report
	#		-	Updated daily
	#		-	Tab-delimited flat file. The report identifies sellable items that will be 365 days or older during the next Long-Term Storage cleanup event, 
	#			if the report is generated within six weeks of the cleanup event date. The report includes both sellable and unsellable items. Content updated 
	#			daily. For FBA sellers. For Marketplace and Seller Central sellers.
	'_GET_FBA_RECOMMENDED_REMOVAL_DATA_'=>524288,

	##	FBA Removal Order Detail Report
	#		- Updatede in real Time
	#		-	Tab-delimited flat file. This report contains all the removal orders, including the items in each removal order, placed during any given time 
	#			period. This can be used to help reconcile the total number of items requested to be removed from an Amazon fulfillment center with the actual 
	#			number of items removed along with the status of each item in the removal order. Content updated in near real-time. For FBA sellers. For 
	#			Marketplace and Seller Central sellers.
	'_GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA_'=>1048576,

	##	FBA Removal Shipment Detail Report
	#		-	Updated in real time
	#		-	Tab-delimited flat file. This report provides shipment tracking information for all removal orders and includes the items that have been 
	#			removed	from an Amazon fulfillment center for either a single removal order or for a date range. This report will not include canceled returns 
	#			or disposed items;	it is only for shipment information. Content updated in near real-time. For FBA sellers. For Marketplace and Seller Central 
	#			sellers.
	'_GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA_'=>2097152,
	);


1;

