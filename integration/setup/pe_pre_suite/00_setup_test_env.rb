step "Determine host OS's" do
  os_families = hosts.inject({}) do |result, host|
    result[host.name] = get_os_family(host)
    result
  end

  ClassifierExtensions.initialize_test_config(options, os_families)
end
