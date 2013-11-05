require 'json'
require 'spec_helper'
require 'puppet'
require 'puppet/indirector/node/classifier'

describe Puppet::Node::Classifier do
  before(:each) do
    Puppet::Node.indirection.terminus_class = :classifier
    allow(Puppet::Node.indirection.terminus).to receive(:config) {{
      :server => 'classifier',
      :port => '9000'
    }}
  end

  NODE_BASE = '/v1/classified/nodes'

  it "makes a GET request to #{NODE_BASE}/nodename" do
    node = 'test'
    connection = double 'connection'
    expect(Puppet::Network::HTTP::Connection).to receive(:new) { connection }
    expect(connection).to receive(:get).with("#{NODE_BASE}/#{node}")
    Puppet::Node.indirection.find(node)
  end

  it "returns a node with the environment and classes given by the remote classifier" do
    node_json = {:name => 'test', :classes => ['main'], :parameters => [], :environment => :dev}.to_json
    response = Net::HTTPOK.new('1.1', 200, 'OK')
    allow(response).to receive(:body) { node_json }
    connection = double 'connection'
    expect(Puppet::Network::HTTP::Connection).to receive(:new) { connection }
    expect(connection).to receive(:get).with("#{NODE_BASE}/test") { response }

    node = Puppet::Node.indirection.find('test')

    expect(node.name).to eq('test')
    expect(node.environment.name).to eq(:dev)
    expect(node.classes).to eq(['main'])
  end
end
