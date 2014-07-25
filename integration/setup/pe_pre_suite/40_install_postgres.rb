step "install and configure postgres on the classifier node" do
  #If the classifier is installed on the same machine as the master, then
  #the classifier should  use the pe-postgres user to set up the classifier's
  #database.
  if classifier == master
    create_pe_classifier_db_with_pe_postgres(host)
  else
    install_postgres(classifier)
  end
end
