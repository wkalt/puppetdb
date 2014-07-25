step "Install the postgres module" do
    if classifier != master
      on classifier, "puppet module install puppetlabs/postgresql --version 3.3.0 --ignore-requirements"
    else 
      on classifier, "puppet module upgrade puppetlabs/postgresql --version 3.3.0 --force"
    end
end
