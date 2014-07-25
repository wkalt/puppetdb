require 'httparty'
require 'uuidtools'

test_name "Terminus runs without config file"

class Classifier
  include HTTParty
  debug_output($stdout)
  headers({'Content-Type' => 'application/json'})
end
Classifier.base_uri("#{classifier.reachable_name}:#{CLASSIFIER_PORT}")


step "Set up puppet without a classifier.yaml"

testdir = master.tmpdir('test_without_terminus_config')
create_remote_file(master, "#{testdir}/site.pp", <<-PP)
class foo {
  notify { "classified as foo": }
}
PP
on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

remove_terminus_config(master)

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'manifest' => "#{testdir}/site.pp",
    'basemodulepath' => "#{testdir}/modules",
    'modulepath' => "#{testdir}/modules",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Run puppet"

stub_hosts_on(master, 'classifier' => fact_on(classifier, 'ipaddress'))
with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("Server hostname 'classifier' did not match server cert", stderr)
    assert_no_match(/No such file or directory - .*\/classifier\.yaml/, stderr)
  end
end
