step "Install the postgres module" do
    on database, "puppet module upgrade puppetlabs/postgresql --version 3.3.0 --force"
end
