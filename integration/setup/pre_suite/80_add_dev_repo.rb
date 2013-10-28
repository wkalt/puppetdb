if (test_config[:install_type] == :package)
  os = test_config[:os_families][database.name]

  base_url = "#{test_config[:package_repo_url]}/#{test_config[:git_ref]}/repo_configs/"
  step "Add development repository on classifier server" do
    case os
    when :debian
      result = on database, "lsb_release -sc"
      deb_flavor = result.stdout.chomp
      apt_list_url = "#{base_url}/deb/pl-classifier-#{test_config[:git_ref]}-#{deb_flavor}.list"
      apt_list_file_path = "/etc/apt/sources.list.d/classifier-prerelease.list"
      on database, "curl \"#{apt_list_url}\" -o #{apt_list_file_path}"
      result = on database, "cat #{apt_list_file_path}"
      Log.notify("APT LIST FILE CONTENTS:\n#{result.stdout}\n")
      on database, "apt-get update"
    when :redhat
      # TODO: this code assumes that we are always running a 64-bit CentOS.  Will
      #  break with Fedora.
      result = on database, "facter operatingsystemmajrelease"
      el_version = result.stdout.chomp
      yum_repo_url = "#{base_url}/rpm/pl-classifier-#{test_config[:git_ref]}-el-#{el_version}-x86_64.repo"
      yum_repo_file_path = "/etc/yum.repos.d/puppetlabs-prerelease.repo"
      on database, "curl \"#{yum_repo_url}\" -o #{yum_repo_file_path}"

      result = on database, "cat #{yum_repo_file_path}"
      Log.notify("Yum REPO DEFINITION:\n\n#{result.stdout}\n\n")
    else
      raise ArgumentError, "Unsupported OS '#{os}'"
    end
  end
end
