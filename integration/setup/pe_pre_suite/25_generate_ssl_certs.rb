step "Run an agent to create the SSL certs" do
  hostname = on(master, 'facter hostname').stdout.strip
  fqdn = on(master, 'facter fqdn').stdout.strip

  with_puppet_running_on(master, :main => { :dns_alt_names => "puppet,#{hostname},#{fqdn}", :autosign => true, :verbose => true, :daemonize => true }) do
    on agents, puppet_agent("--test")
    on(classifier, "puppet agent -t")
  end
end
