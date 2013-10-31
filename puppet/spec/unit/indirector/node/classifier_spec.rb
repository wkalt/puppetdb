require 'json'
require 'spec_helper'
require 'puppet'
require 'puppet/indirector/node/classifier'

describe Puppet::Node::Classifier do
  before(:each) do
    Puppet::Node.indirection.terminus_class = :classifier
  end

  it "uses the classifier_{server,port} settings from the puppet settings" do
    server = "test_server.localdomain"
    port = 8256
    Puppet[:classifier_server] = server
    Puppet[:classifier_port] = port

    Puppet::Network::HTTP::Connection.expects(:new).with(server, port).returns(stub 'connection', :get=>nil)

    Puppet::Node.indirection.find('test')
  end

  it "makes a GET request to /v1/node/nodename" do
    node = 'test'
    connection = mock 'connection'
    Puppet::Network::HTTP::Connection.expects(:new).returns(connection)
    connection.expects(:get).with("/v1/node/#{node}")
    Puppet::Node.indirection.find(node)
  end

  it "returns a node with the environment and classes given by the remote classifier" do
    node_json = {:name => 'test', :classes => ['main'], :parameters => [], :environment => :dev}.to_json
    response = Net::HTTPOK.new('1.1', 200, 'OK')
    response.stubs(:body).returns(node_json)
    connection = mock 'connection'
    Puppet::Network::HTTP::Connection.expects(:new).returns(connection)
    connection.expects(:get).with("/v1/node/test").returns(response)

    node = Puppet::Node.indirection.find('test')

    expect(node.name).to eq('test')
    expect(node.environment.name).to eq(:dev)
    expect(node.classes).to eq(['main'])
  end
end
