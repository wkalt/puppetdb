step "install and configure postgres on the classifier node" do
  #If the classifier is installed on the same machine as the master, then
  #the classifier should  use the pe-postgres user to set up the classifier's
  #database.
  if classifier == master
    create_pe_classifier_db_with_pe_postgres(classifier)
  else
    install_postgres(classifier)
  end

  create_databases_on(classifier,
                      [{:database => "pe-classifier",
                        :user => "pe-classifier",
                        :password => "pe-classifier"},
                       {:database => "rbac",
                        :user => "rbac",
                        :password => "rbac"},
                       {:database => "activity",
                        :user => "activity",
                        :password => "activity"}])
end
