step "Install the postgres module" do
  on classifier, "puppet module install puppetlabs/postgresql --version 3.3.0"
end