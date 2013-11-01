step "Run an agent to create the SSL certs" do
  with_puppet_running_on master, {'master' => {'autosign' => 'true'}} do
    on agents, puppet_agent("--test")
  end
end
