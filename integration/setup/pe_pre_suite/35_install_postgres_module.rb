step "Install the postgres module" do
    on database, "puppet module upgrade puppetlabs/postgresql"
end
