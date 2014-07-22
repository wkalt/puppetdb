step "install the classifier from the package" do
  #if installing the classifier on a separate box, it will need pe-java
  if classifier != master
    os = test_config[:os_families][classifier.name]
    packages_filepath = "#{classifier['working_dir']}/#{classifier['dist']}/packages/#{classifier['platform']}"
    case os
    when :debian
      create_remote_file(classifier, '/etc/apt/sources.list.d/pl_tarball.list', "deb file:#{packages_filepath} ./")
      on classifier, 'apt-get update'
      on classifier, 'apt-get install -y pe-java'
    when :redhat
      on classifier, "yum install -y #{packages_filepath}/pe-java*.rpm"
    end
  end
  install_classifier(classifier)

end
