# Don't currently need this I think?? may need os family stuff later for
# the install
os_families = {}

step "Determine host OS's" do
  os_families = hosts.inject({}) do |result, host|
    result[host.name] = get_os_family(host)
    result
  end
end

ClassifierExtensions.initialize_test_config(options, os_families)
