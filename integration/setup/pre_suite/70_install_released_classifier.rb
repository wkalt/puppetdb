if (test_config[:install_mode] == :upgrade)
  step "Install most recent released classifier on the classifier server for upgrade test" do
    install_classifier(database, test_config[:database], 'latest')
    start_classifier(database)
    install_classifier_termini(master, database, 'latest')
  end
end
