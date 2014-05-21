step "Install the postgres module" do
    on database, "puppet module install puppetlabs/postgresql --force"
end
