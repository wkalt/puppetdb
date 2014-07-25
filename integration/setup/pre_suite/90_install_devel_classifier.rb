step "Install development build of classifier on the classifier server" do
  os = test_config[:os_families][classifier.name]

  case test_config[:install_type]
  when :git
    raise "No CLASSIFIER_REPO_CLASSIFIER set" unless test_config[:repo_classifier]

    case os
    when :redhat
      on classifier, "yum install -y git-core ruby rubygem-rake"
    when :debian
      on classifier, "apt-get install -y git-core ruby rake"
    else
      raise "OS #{os} not supported"
    end

    on classifier, "rm -rf #{GitReposDir}/classifier"
    repo = extract_repo_info_from(test_config[:repo_classifier].to_s)
    install_from_git classifier, GitReposDir, repo

    install_postgres(classifier)
    install_classifier_from_source(classifier)
    install_classifier_terminus_from_source(master, classifier)
  when :package
    Log.notify("Installing classifier from package; install mode: '#{test_config[:install_mode].inspect}'")

    install_classifier(classifier)

    if test_config[:validate_package_version]
      validate_package_version(classifier)
    end

    install_classifier_terminus(master, classifier)
  end
end
