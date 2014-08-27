require 'json'
require 'spec_helper'
require 'puppet'
require 'puppet/indirector/node/classifier'

describe Puppet::Node::Classifier do
  before(:each) do
    Puppet::Node.indirection.terminus_class = :classifier
    allow(Puppet::Node.indirection.terminus).to receive(:config) {{
      :server => 'classifier',
      :port => '9000',
      :prefix => '/api',
    }}
  end

  NODE_BASE = '/v1/classified/nodes'

  it "returns a node with the environment and classes given by the remote classifier" do
    node_json = {:name => 'test', :classes => ['main'], :parameters => {}, :environment => :dev}.to_json
    response = Net::HTTPOK.new('1.1', 200, 'OK')
    allow(response).to receive(:body) { node_json }
    connection = double 'connection'
    expect(Puppet::Network::HttpPool).to receive(:http_instance) { connection }
    expect(connection).to receive(:post).with("/api#{NODE_BASE}/test", anything(), kind_of(Hash)) { response }

    node = Puppet::Node.indirection.find('test')

    expect(node.name).to eq('test')
    expect(node.environment.name).to eq(:dev)
    expect(node.classes).to eq(['main'])
  end

  it "works when no facts are available" do
    node_json = {:name => 'test', :classes => ['main'], :parameters => {}, :environment => :dev}.to_json
    response = Net::HTTPOK.new('1.1', 200, 'OK')
    allow(response).to receive(:body) { node_json }
    connection = double 'connection'
    expect(Puppet::Network::HttpPool).to receive(:http_instance) { connection }
    expect(connection).to receive(:post).with("/api#{NODE_BASE}/test", anything(), kind_of(Hash)) { response }
    allow(Puppet::Node::Facts.indirection).to receive(:find) { nil }

    node = Puppet::Node.indirection.find('test')

    expect(node.name).to eq('test')
    expect(node.environment.name).to eq(:dev)
    expect(node.classes).to eq(['main'])
  end
end
