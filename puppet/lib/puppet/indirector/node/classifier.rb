require 'puppet/network/http_pool'
require 'json'

class Puppet::Node::Classifier < Puppet::Indirector::Code
  def new_connection
    Puppet::Network::HttpPool.http_instance(server, port)
  end

  def find(request)
    connection = new_connection
    node_name = request.key

    facts = Puppet::Node::Facts.indirection.find(node_name,
                                                 :environment => request.environment)
    facts.sanitize

    trusted_data = Puppet.lookup(:trusted_information) do
      # This block contains a default implementation for trusted
      # information. It should only get invoked if the node is local
      # (e.g. running puppet apply)
      temp_node = Puppet::Node.new(node_name)
      temp_node.parameters['clientcert'] = Puppet[:certname]
      Puppet::Context::TrustedInformation.local(temp_node)
    end.to_h

    request_body = {"facts" => facts.to_data_hash['values'],
                    "trusted" => trusted_data}

    if request.options.include?(:transaction_uuid)
      request_body["transaction_uuid"] = request.options[:transaction_uuid]
    end

    response = connection.post("/v1/classified/nodes/#{node_name}", request_body.to_json)

    node = nil
    if response.is_a? Net::HTTPSuccess
      result = JSON.parse(response.body)
      node = Puppet::Node.from_data_hash(result)
      node.fact_merge
    else
      response.error!
    end

    node
  end

  private

  def server
    config[:server]
  end

  def port
    config[:port]
  end

  def config
    @config ||= load_config
  end

  def load_config
    config = YAML.load_file(File.join(Puppet[:confdir], 'classifier.yaml'))

    {
      :server => config["server"] || 'classifier',
      :port => config["port"] || 1262,
    }
  end
end
