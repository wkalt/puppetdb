step "Install development build of classifier on the classifier server" do
  os = test_config[:os_families][database.name]

  case test_config[:install_type]
  when :git
    raise "No CLASSIFIER_REPO_CLASSIFIER set" unless test_config[:repo_classifier]

    case os
    when :redhat
      on database, "yum install -y git-core ruby rubygem-rake"
    when :debian
      on database, "apt-get install -y git-core ruby rake"
    else
      raise "OS #{os} not supported"
    end

    on database, "rm -rf #{GitReposDir}/classifier"
    repo = extract_repo_info_from(test_config[:repo_classifier].to_s)
    install_from_git database, GitReposDir, repo

    if (test_config[:database] == :postgres)
      install_postgres(database)
    end
    install_classifier_via_rake(database)
    start_classifier(database)
    install_classifier_termini_via_rake(master, database)
  when :package
    Log.notify("Installing classifier from package; install mode: '#{test_config[:install_mode].inspect}'")

    install_classifier(database, test_config[:database])

    if test_config[:validate_package_version]
      validate_package_version(database)
    end

    install_classifier_termini(master, database)

    start_classifier(database)
  end
end
