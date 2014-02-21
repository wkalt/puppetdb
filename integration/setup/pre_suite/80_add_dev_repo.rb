if (test_config[:install_type] == :package)

  step "Add development repository on classifier server" do
    install_dev_repos_on("classifier", database, test_config[:git_ref], "repo_configs")
  end
end
