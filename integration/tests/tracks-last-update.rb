require 'time'

test_name "the classifier tracks the time of the most recent update from puppet"

# disable class sync
conf = {'classifier' => {'synchronization-period' => 0}}
write_conf_file(classifier, "sync.conf", conf)
clear_and_restart_classifier(classifier)

testdir = master.tmpdir('test_last_update')

on master, "mkdir -p #{testdir}/environments/production/manifests"

create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", <<-PP)
class param_test($a) {
  notify {"Parameter a: $a": }
}

notify {"Hello: $hello": }
PP

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'environmentpath' => "#{testdir}/environments",
    'environment_timeout' => 0,
    'rest_authconfig' => "#{testdir}/auth.conf",
    'basemodulepath' => "#{testdir}/modules",
    'modulepath' => "#{testdir}/modules",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Verify that initially the last_update time is null"

initial_time = Classifier.get("/v1/last-class-update")

assert(initial_time['last_update'].nil?, "Expected last_update to start null. Response was #{initial_time}")

step "Update classes from puppet"

start_time = Time.now()
sleep(1)

with_puppet_running_on(master, master_opts, testdir) do
  update_response = Classifier.post("/v1/update-classes")
  assert(update_response.response.is_a?(Net::HTTPSuccess),
         "Received failure response when trying to update classes: " +
         "HTTP CODE #{update_response.code}: #{update_response.message}")
end

sleep(1)
end_time = Time.now()

step "Verify that the last_update time reflects the update"

final_time = Classifier.get("/v1/last-class-update")

last_update = Time.parse(final_time['last_update'])

assert((start_time < last_update) && (last_update < end_time),
       "Last update of #{last_update.utc} was not in the expected range of #{start_time.utc}..#{end_time.utc}.")
