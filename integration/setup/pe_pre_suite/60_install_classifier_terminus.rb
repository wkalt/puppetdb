step "Add development repository on puppet master" do
  install_dev_repos_on("pe-classifier", master, test_config[:git_ref], "repo_configs")

  install_classifier_terminus(master, classifier)

  on master, "service pe-httpd restart"
end
