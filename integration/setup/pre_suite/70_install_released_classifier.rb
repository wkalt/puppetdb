if (test_config[:install_mode] == :upgrade)
  step "Install most recent released classifier on the classifier server for upgrade test" do
    install_classifier(classifier)
    install_classifier_terminus(master, classifier)
  end
end
