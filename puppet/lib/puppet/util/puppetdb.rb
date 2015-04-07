require 'puppet/util'
require 'puppet/util/logging'
require 'puppet/util/profiler'
require 'puppet/util/puppetdb/command_names'
require 'puppet/util/puppetdb/command'
require 'puppet/util/puppetdb/config'
require 'digest/sha1'
require 'time'
require 'fileutils'

module Puppet::Util::Puppetdb

  class CommandSubmissionError < Puppet::Error
    def initialize(msg, context)
      super(msg)
      @context = context
    end
  end

  class InventorySearchError < Puppet::Error
  end
  class SoftWriteFailError < Puppet::Error
  end


  def self.config
    @config ||= Puppet::Util::Puppetdb::Config.load
    @config
  end

  def self.puppet3compat?
    defined?(Puppet::Parser::AST::HashOrArrayAccess)
  end

  # Given an instance of ruby's Time class, this method converts it to a String
  # that conforms to PuppetDB's wire format for representing a date/time.
  def self.to_wire_time(time)
    # The current implementation simply calls iso8601, but having this method
    # allows us to change that in the future if needed w/o being forced to
    # update all of the date objects elsewhere in the code.
    time.iso8601(9)
  end

  # Convert a value (usually a string) to a boolean
  def self.to_bool(value)
    case value
    when true, "true"; return true
    when false, "false"; return false
    else
      raise ArgumentError.new("invalid value for Boolean: \"#{val}\"")
    end
  end

  # @!group Public instance methods
  def underscores_to_dashes(m)
    Hash[m.map {|k, v| [k.gsub("_","-"), v]}]
  end

  def legacy_command_versions
    {"replace facts" => 1, "replace catalog" => 3, "store report" => 2}
  end

  def stringify(values)
    values.each do |fact,value|
      values[fact] = value.to_s
    end
  end

  def munge_facts_command(command)
    stringify(command["values"])
    {"name" => command["certname"],
     "values" => stringify(command["values"])}.to_json
  end

  def munge_reports_command(command)
    {"certname" => command["certname"],
     "puppet-version" => command["puppet_version"],
     "report-format" => command["report_format"],
     "configuration-version" => command["configuration_version"],
     "start-time" => command["start_time"],
     "end-time" => command["end_time"],
     "resource-events" => command["resource_events"].map {|x| underscores_to_dashes(x)},
     "transaction-uuid" => command["transaction_uuid"]}
  end

  def munge_catalogs_command(command)
    {"data" => {"name" => command["certname"],
                "version" => command["version"],
                "transaction-uuid" => command["transaction_uuid"],
                "edges" => command["edges"],
                "resources" => command["resources"]},
    "metadata" => {"api_version" => 1}}
  end

  def munge_command(payload)
    command = JSON.parse(payload)
    command_type = command["command"]
    case command_type
    when "replace facts"
      payload = munge_facts_command(command["payload"])
    when "store report"
      payload = munge_reports_command(command["payload"])
    when "replace catalog"
      payload = munge_catalogs_command(command["payload"])
    end

    {"command" => command_type,
      "version" => legacy_command_versions[command_type],
      "payload" => payload}.to_json
  end

  # Submit a command to PuppetDB.
  #
  # @param certname [String] hostname name of puppetdb instance
  # @param payload [String] payload
  # @param command_name [String] name of command
  # @param version [Number] version number of command
  # @return [Hash <String, String>]
  def submit_command(certname, payload, command_name, version)
    profile("Submitted command '#{command_name}' version '#{version}'",
            [:puppetdb, :command, :submit, command_name, version]) do
      command = Puppet::Util::Puppetdb::Command.new(command_name, version, certname, payload)
      command.submit
    end
  end

  # Profile a block of code and log the time it took to execute.
  #
  # This outputs logs entries to the Puppet masters logging destination
  # providing the time it took, a message describing the profiled code
  # and a leaf location marking where the profile method was called
  # in the profiled hierachy.
  #
  # @param message [String] A description of the profiled event
  # @param metric_id [Array] A list of strings making up the ID of a metric to profile
  # @param block [Block] The segment of code to profile
  # @api public
  def profile(message, metric_id, &block)
    message = "PuppetDB: " + message
    arity = Puppet::Util::Profiler.method(:profile).arity
    case arity
    when 1
      Puppet::Util::Profiler.profile(message, &block)
    when 2, -2
      Puppet::Util::Profiler.profile(message, metric_id, &block)
    end
  end

  # @!group Private instance methods

  # @api private
  def config
    Puppet::Util::Puppetdb.config
  end

  # @api private
  def log_x_deprecation_header(response)
    if warning = response['x-deprecation']
      Puppet.deprecation_warning "Deprecation from PuppetDB: #{warning}"
    end
  end
  module_function :log_x_deprecation_header

end
