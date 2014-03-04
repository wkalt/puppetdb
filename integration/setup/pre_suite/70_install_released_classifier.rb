if (test_config[:install_mode] == :upgrade)
  step "Install most recent released classifier on the classifier server for upgrade test" do
    install_classifier(database)
    start_classifier(database)
    install_classifier_terminus(master, database)
  end
end
