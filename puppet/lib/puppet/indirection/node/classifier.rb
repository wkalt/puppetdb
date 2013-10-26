require 'puppet/network/http/connection'

Puppet.define_settings(
  :master,
  :classifier_server => {
    :default => "classifier",
    :desc => "The hostname to use when contacting the classifier service."
  },
  :classifier_port => {
    :default => 8080,
    :desc => "The port to use when contacting the classifier service."
  })

class Puppet::Node::Classifier < Puppet::Indirector::Code
  def new_connection
    Puppet::Network::HTTP::Connection.new(Puppet[:classifier_server], Puppet[:classifier_port])
  end

  def find(request)
    connection = new_connection

    response = connection.get("/v1/node/#{request.key}")

    if response.is_a? Net::HTTPSuccess
      result = PSON.parse(response.body)
      Puppet::Node.from_pson(result)
    end
  end
end
