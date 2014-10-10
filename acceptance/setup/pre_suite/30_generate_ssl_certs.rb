pp 'before 30_ssl'
pp Dir['/var/run/puppet/*']
step "Run an agent to create the SSL certs" do
  with_puppet_running_on(
    master,
    :master => {:autosign => 'true', :trace => 'true'}) do
    run_agent_on(database, "--test")
  end
end
pp 'after 30_ssl'
pp Dir['/var/run/puppet/*']
