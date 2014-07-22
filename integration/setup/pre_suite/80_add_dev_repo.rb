if (test_config[:install_type] == :package)

  step "Add development repository on classifier server" do
    install_dev_repos_on("classifier", classifier, test_config[:git_ref], "repo_configs")
    install_dev_repos_on("classifier", master, test_config[:git_ref], "repo_configs")
  end
end
