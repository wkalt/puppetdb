test_name "Install Puppet" do
  step "Install Puppet" do
    install_puppet
  end

  step "Populate facts from each host" do
    populate_facts
  end

  pidfile = '/var/run/puppet/master.pid'

  master_facts = facts(master.name)

  with_puppet_running_on(
    master,
    :master => {:dns_alt_names => "puppet,#{master_facts['hostname']},#{master_facts['fqdn']}",
                :trace => 'true'},
    :commandline_args => '--debug') do
    # PID file exists?
    step "PID file created?" do
      on master, "[ -f #{pidfile} ]"
    end
  end
end
