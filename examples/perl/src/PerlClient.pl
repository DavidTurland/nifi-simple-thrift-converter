#!/usr/bin/env perl

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
# https://www.javadoc.io/static/org.apache.nifi/nifi-api/1.7.0/index.html?org/apache/nifi/flowfile/FlowFile.html
# https://fastapi.metacpan.org/source/JKING/Thrift-0.16.0/t/processor.t

use strict;
use warnings;

use Encode;
use LWP::UserAgent;
use strict;
use warnings;

use Data::Dumper;

use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::HttpClient;
use Thrift::Type;

use lib 'gen-perl';

use Org::Apache::Nifi::Processors::Thrift::Types;
use Org::Apache::Nifi::Processors::Thrift::Constants;
use Benchmark qw() ;
use Test::More tests => 3;

my $self;

{
    my $initted;
    sub _init{
        my $initted;
        return if $initted;
        $self = {timeout => 120 *1000,
                url     => 'http://localhost:9090/',
                headers => undef,
                };
        #$self->{transport}  = Thrift::MemoryBuffer->new(10000);
        $self->{transport}  = Thrift::HttpClient->new();
        delete $self->{transport}{headers};
        #$self->{transport}->setHeader('Content-Type','application/x-thrift');
        $self->{protocol}   = Thrift::BinaryProtocol->new($self->{transport});
        my $self = {
                headers      => {},
        };
        $self->{headers}->{foo} = 'bar';
        print "ref headers is " . ref ($self->{headers});

        #$self->{ua} = LWP::UserAgent->new(
        #    'timeout' => ($self->{timeout} // 1000),
        #    'agent'   => 'Perl/THttpClient'
        #);
        #$self->{ua}->default_header('Accept' => 'application/x-thrift');
        #$self->{ua}->default_header('Content-Type' => 'application/x-thrift');
        #$self->{ua}->cookie_jar({}); # hash to remember cookies between redirects
        $initted = 1;
    }
}

subtest 'simple http client' => sub {
    _init();
#    plan tests => 4;
#    my $attributes = {foo => "barzz",};
#    my $id = 5;
#    my $flow_file = Org::Apache::Nifi::Processors::ThriftFlowFile->new({
#                                      content    => '0123456789' x 10,
#                                      attributes => $attributes, });
#
#    my $ffr_a = Org::Apache::Nifi::Processors::Thrift::FlowFileRequest->new({id         => $id,
#                                                                     flowFile    => $flow_file, });
#    my $expected_response_code = 200;
#    $ffr_a->write($self->{protocol});
#    my $ffr_b = Org::Apache::Nifi::Processors::FlowFileRequest->new();
#    $ffr_b->read($self->{protocol});
#
#    my $request = HTTP::Request->new(POST => $self->{url}, ($self->{headers} || undef), $self->{transport}->getBuffer() );
#    my $response = $self->{ua}->request($request);
#
#    $self->{transport}->resetBuffer($response->content());
#
#    my $ffr_c = Org::Apache::Nifi::Processors::Thrift::FlowFileReply->new();
#    eval{
#        $ffr_c->read($self->{protocol});
#        is($response->code,$expected_response_code,         'correct response code');
#        is(       $ffr_c->{id}        ,$ffr_a->{id},        'correct id');
#        is(       $ffr_c->{content}   ,$ffr_a->{content},   'correct content');
#        is_deeply($ffr_c->{attributes},$ffr_a->{attributes},'correct attributes');
#    };
#    if($@){
#        fail("failed with " . $@);
#    }
};

subtest  'simple_http_send' => sub {
    _init();
    plan tests => 4;
    my $expected_response_code = 200;
    my $attributes = {foo => "barzz",};
    my $id = 5;
    my $flow_file = Org::Apache::Nifi::Processors::Thrift::ThriftFlowFile->new({
                                      content    => '0123456789' x 10,
                                      attributes => $attributes, });

    my $ffr_a = Org::Apache::Nifi::Processors::Thrift::FlowFileRequest->new({id         => $id,
                                                                     flowFile    => $flow_file, });
    #print Dumper($ffr_a);
    eval{
        my ($ffr_c,$rc) = _send($ffr_a,0);
        print "got this far\n";
        print Dumper($ffr_c);
        my $flow_file_response = $ffr_c->{flowFile};
        is(       $rc,                 $expected_response_code, 'correct response code');
        is(       $ffr_c->{id}        ,$ffr_a->{id},            'correct id');
        is(       $flow_file_response->{content}   ,$flow_file->{content},       'correct content');
        is_deeply($flow_file_response->{attributes},$flow_file->{attributes},    'correct attributes');
    };
    if($@){
        fail("failed with " . $@);
    }
};

#subtest 'benchmark' => sub {
#    _init();
#    plan tests => 1;
#    eval{
#        my $id = 5;
#        my $attributes = {foo => "bar",};
#        my $flow_file = Org::Apache::Nifi::Processors::Thrift::ThriftFlowFile->new({
#                                          content    => '0123456789' x 10,
#                                          attributes => $attributes, });
#
#        my $ffr = Org::Apache::Nifi::Processors::Thrift::FlowFileRequest->new({id         => $id,
#                                                                         flowFile    => $flow_file, });
#        my $count = -1; # 1 second
#        Benchmark::timethese($count, {
#            'simple' => sub { my $resp = _send($ffr); },
#        });
#        pass "finished()";
#    };
#    if($@){
#        warn(Dumper($@));
#        fail("failed with " . $@);
#    }
#};

sub _send{
    my ($flowFileRequest,$debug) = @_;
    ## $self->{transport}->resetBuffer();
    $flowFileRequest->write($self->{protocol});

    if ($debug){
        my $ffr_b = Org::Apache::Nifi::Processors::Thrift::FlowFileRequest->new();
        #$out->setpos(0); # rewind
        $ffr_b->read($self->{protocol});
        my $flow_file_request = $ffr_b->{flowFile};
        print Dumper($flow_file_request);
    }
    $self->{transport}->flush();

    ##my $request  = HTTP::Request->new(POST => $self->{url}, ($self->{headers} || undef), $self->{transport}->getBuffer() );
    ##my $response = $self->{ua}->request($request);
    ##$self->{transport}->resetBuffer($response->content());
    my $flowFileReply = Org::Apache::Nifi::Processors::Thrift::FlowFileReply->new();
    $flowFileReply->read($self->{protocol});
    print Dumper($flowFileReply);
    #return wantarray? ($flowFileReply,$response->code):$flowFileReply;
    return ($flowFileReply,200);
}
