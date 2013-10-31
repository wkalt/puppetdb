require 'puppet/network/http/connection'

class Puppet::Node::Classifier < Puppet::Indirector::Code
  def new_connection
    Puppet::Network::HTTP::Connection.new(server, port, :use_ssl => false)
  end

  def find(request)
    connection = new_connection

    response = connection.get("/v1/node/#{request.key}")

    if response.is_a? Net::HTTPSuccess
      result = PSON.parse(response.body)
      Puppet::Node.from_pson(result)
    end
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
