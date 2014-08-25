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

    fact_values = if facts.nil?
                    {}
                  else
                    facts.sanitize
                    facts.to_data_hash['values']
                  end

    trusted_data = Puppet.lookup(:trusted_information) do
      # This block contains a default implementation for trusted
      # information. It should only get invoked if the node is local
      # (e.g. running puppet apply)
      temp_node = Puppet::Node.new(node_name)
      temp_node.parameters['clientcert'] = Puppet[:certname]
      Puppet::Context::TrustedInformation.local(temp_node)
    end

    trusted_data_values = if trusted_data.nil?
                            {}
                          else
                            trusted_data.to_h
                          end

    request_body = {"fact" => fact_values,
                    "trusted" => trusted_data_values}

    if request.options.include?(:transaction_uuid)
      request_body["transaction_uuid"] = request.options[:transaction_uuid]
    end

    response = connection.post("/v1/classified/nodes/#{node_name}", request_body.to_json, 'Content-Type' => 'application/json')

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
    config_path = File.join(Puppet[:confdir], 'classifier.yaml')
    if File.exists?(config_path)
      config = YAML.load_file(config_path)
    else
      Puppet.warning("Classifier config file '#{config_path}' does not exist, using defaults")
      config = {}
    end

    {
      :server => config["server"] || 'classifier',
      :port => config["port"] || 1262,
    }
  end
end
