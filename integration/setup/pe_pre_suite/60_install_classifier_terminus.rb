step "Add development repository on puppet master" do
  install_classifier_terminus(master, classifier)

  on master, "service pe-httpd restart"
end
