step "Add development repository" do
  # It needs to be on both classifier and master (if different) so the
  # terminus can be installed on the master

  [classifier, master].uniq.each do |host|
    install_dev_repos_on("pe-console-services", host, test_config[:git_ref], "repo_configs")
  end
end
