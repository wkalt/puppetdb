require 'puppet/network/http_pool'

class Puppet::Node::Classifier < Puppet::Indirector::Code
  def new_connection
    Puppet::Network::HttpPool.http_instance(server, port, false, false)
  end

  def find(request)
    connection = new_connection

    response = connection.get("/v1/classified/nodes/#{request.key}")

    node = nil
    if response.is_a? Net::HTTPSuccess
      result = PSON.parse(response.body)
      node = Puppet::Node.from_pson(result)
      node.fact_merge
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
      :port => config["port"] || 8080,
    }
  end
end
