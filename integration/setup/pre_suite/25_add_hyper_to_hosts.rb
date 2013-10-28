step "Add hypervisor to hosts" do
  hosts.each do |host|
    on host, 'echo "`ip route | grep default | cut -d\  -f3` hypervisor" >> /etc/hosts'
  end
end
