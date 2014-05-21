step "Add development repository on classifier server" do
  install_dev_repos_on("pe-classifier", database, test_config[:git_ref], "repo_configs")
end
